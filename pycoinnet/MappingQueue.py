import asyncio
import logging


class TaskGroup:
    def __init__(self, repeated_f, worker_count=1, done_callback=None, loop=None):
        loop = loop or asyncio.get_event_loop()
        if not asyncio.iscoroutinefunction(repeated_f):
            raise ValueError("repeated_f must be an async coroutine")

        if done_callback and not asyncio.iscoroutinefunction(done_callback):
            raise ValueError("done_callback must be an async coroutine")

        self._repeated_f = repeated_f
        self._is_stopping = asyncio.Event()
        self._is_immediate = False
        self._task = loop.create_task(self._run(worker_count, done_callback, loop))

    async def _run(self, worker_count, done_callback, loop):
        self._tasks = set()
        for _ in range(worker_count):
            self._tasks.add(loop.create_task(self._worker()))
        await asyncio.wait(self._tasks)
        if done_callback:
            await loop.create_task(done_callback())

    async def _worker(self):
        done_future = self._is_stopping.wait()
        while True:
            repeated_future = asyncio.ensure_future(self._repeated_f(self))
            done, pending = await asyncio.wait(
                [done_future, repeated_future], return_when=asyncio.FIRST_COMPLETED)
            if self._is_immediate or not repeated_future.done():
                break

            try:
                await repeated_future
            except Exception as ex:
                logging.exception("problem in worker thread")

        if not repeated_future.done():
            repeated_future.cancel()

    def cancel(self, immediate=False):
        if not self._is_stopping.is_set():
            self._is_immediate = immediate
            self._is_stopping.set()

    async def join(self):
        await self._task



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
        """

        # build the queues
        queues = []
        for arg in args:
            input_q = arg.get("input_q")
            input_q_maxsize = arg.get("input_q_maxsize", 0)

            if input_q and input_q_maxsize:
                raise ValueError("at most one of input_q and input_q_maxsize must be set: %s" % arg)

            q = input_q or asyncio.Queue(maxsize=input_q_maxsize)
            queues.append(q)
        queues.append(final_q or asyncio.Queue())

        def make_repeated_f(input_q, callback_f, output_q):

            async def repeated_f(task_group):
                """
                Process all items. So repeat until queue is confirmed empty.
                """
                while True:
                    item = await input_q.get()
                    f_item = await callback_f(item, output_q)
                    if input_q.empty():
                        break

            return repeated_f

        prior_done_f = None
        for _, d in enumerate(args):
            input_q, output_q = queues[_:_+2]
            callback_f = d.get("callback_f")
            worker_count = d.get("worker_count", 1)
            if not asyncio.iscoroutinefunction(callback_f):
                raise ValueError("callback_f must be an async coroutine")

            repeated_f = make_repeated_f(input_q, callback_f, output_q)

            task_group = TaskGroup(repeated_f, worker_count=worker_count, done_callback=prior_done_f, loop=loop)

            def make_done(tg):

                async def done():
                    tg.cancel()
                    await tg.join()
                return done

            prior_done_f = make_done(task_group)

        self._loop = loop or asyncio.get_event_loop()
        self._in_q = queues[0]
        self._out_q = queues[-1]
        self._done_function = prior_done_f

    async def done(self):
        if getattr(self, "_done_function", None):
            await self._done_function()
            self._done_function = None

    cancel = done

    def __del__(self):
        f = getattr(self, "_done_function", None)
        if f:

            async def callback():
                await f()

            self._loop.create_task(callback())

    async def put(self, item):
        await self._in_q.put(item)

    def put_nowait(self, item):
        self._in_q.put_nowait(item)

    async def get(self):
        return (await self._out_q.get())

    def empty(self):
        return self._out_q.empty()

    def task_done(self):
        return self._out_q.task_done()
