import asyncio
import logging

from concurrent.futures import CancelledError


def _add_stop(q):

    def stop():
        if not q._is_stopping_future.done():
            q._is_stopping_future.set_result(None)

    q._is_stopping_future = asyncio.Future()
    q.stop = stop


def _make_repeated_f(input_q, callback_f, output_q):
    """
    input_q: q to get unprocessed items from
    callback_f: invoked on items from input_q
    output_q: q to put processed items to
    """
    async def repeated_f():
        """
        Process all items. Repeat until queue is empty.
        """

        # if all items are put, fall through to the second loop which
        # does get_nowait
        while not input_q._is_stopping_future.done():
            input_q_get = asyncio.ensure_future(input_q.get())
            done, pending = await asyncio.wait(
                [input_q._is_stopping_future, input_q_get], return_when=asyncio.FIRST_COMPLETED)
            if input_q_get.done():
                item = input_q_get.result()
                try:
                    await callback_f(item, output_q)
                except CancelledError:
                    pass
                except Exception as ex:
                    logging.exception("unhandled MappingQueue task exception")
                input_q.task_done()
            else:
                input_q_get.cancel()

        # all items have been put
        # do a soft finish (non-blocking get) of remaining items
        while not input_q.empty():
            item = input_q.get_nowait()
            await callback_f(item, output_q)
            input_q.task_done()

        # inform the next level that we are done
        output_q.stop()

    return repeated_f


class MappingQueue:
    def __init__(self, *args, final_q=None, loop=None):
        """
        Create a pipeline of queues. q => function => q => function => q => ... => final_q

        Values get "async put" into the queue, and come out some time later after processing
        with "async get".

        each arg is a dictionary with the following optional parameters:
        input_q: the Queue subclass
        input_q_maxsize: the maxsize of the Queue
        worker_count: maximum number of tasks pulling from the queue. Default is 1
        callback_f: a function called with the item and the output_q, into which it may put items

        There are two ways to end:
            stop: that means no more items will be added. Once a queue is empty, the workers at that
                stage terminate and "stop" is sent to the next stage
            cancel: no more results will be processed, so cancel immediately propogates to all stages.
        """
        loop = loop or asyncio.get_event_loop()

        # build the queues
        queues = []
        for arg in args:
            input_q = arg.get("input_q")
            input_q_maxsize = arg.get("input_q_maxsize", 0)

            if input_q and input_q_maxsize:
                raise ValueError("at most one of input_q and input_q_maxsize must be set: %s" % arg)

            q = input_q or asyncio.Queue(maxsize=input_q_maxsize)
            queues.append(q)
            _add_stop(q)

        queues.append(final_q or asyncio.Queue())
        _add_stop(queues[-1])

        def prior_cancel_f(x):
            return None

        for _, d in enumerate(args):
            input_q, output_q = queues[_:_+2]
            callback_f = d.get("callback_f")
            worker_count = d.get("worker_count", 1)
            if not asyncio.iscoroutinefunction(callback_f):
                raise ValueError("callback_f must be an async coroutine")

            repeated_f = _make_repeated_f(input_q, callback_f, output_q)

            task_group = asyncio.gather(*(loop.create_task(repeated_f()) for _ in range(worker_count)))
            task_group.add_done_callback(prior_cancel_f)

            def _make_cancel(task_group):

                def _cancel(f):
                    task_group.cancel()

                return _cancel

            prior_cancel_f = _make_cancel(task_group)

        self._loop = loop
        self._in_q = queues[0]
        self._out_q = queues[-1]
        self._cancel_function = task_group.cancel

    def stop(self):
        self._in_q.stop()

    async def wait(self):
        await self._out_q._is_stopping_future

    async def drain(self):
        self.stop()
        while not self._out_q._is_stopping_future.done():
            await self.get()

    def cancel(self):
        if getattr(self, "_cancel_function", None):
            self._cancel_function()
            self._cancel_function = None

    def __del__(self):
        self.cancel()

    async def put(self, item):
        await self._in_q.put(item)

    def put_nowait(self, item):
        self._in_q.put_nowait(item)

    async def get(self):
        return (await self._out_q.get())

    def get_nowait(self):
        return self._out_q.get_nowait()

    def empty(self):
        return self._out_q.empty()

    def full(self):
        return self._in_q.full()

    def task_done(self):
        return self._out_q.task_done()

    async def join(self):
        await self._in_q.join()
