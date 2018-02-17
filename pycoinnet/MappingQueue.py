import asyncio

from .task_group import TaskGroup


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
                    await callback_f(item, output_q)
                    if input_q.empty():
                        break

            return repeated_f

        prior_cancel_f = None
        for _, d in enumerate(args):
            input_q, output_q = queues[_:_+2]
            callback_f = d.get("callback_f")
            worker_count = d.get("worker_count", 1)
            if not asyncio.iscoroutinefunction(callback_f):
                raise ValueError("callback_f must be an async coroutine")

            repeated_f = make_repeated_f(input_q, callback_f, output_q)

            task_group = TaskGroup(
                repeated_f, worker_count=worker_count, done_callback=prior_cancel_f, loop=loop)

            prior_cancel_f = task_group.cancel

        self._loop = loop or asyncio.get_event_loop()
        self._in_q = queues[0]
        self._out_q = queues[-1]
        self._cancel_function = prior_cancel_f

    def cancel(self):
        if getattr(self, "_cancel_function", None):
            self._cancel_function()
            self._cancel_function = None

    def __del__(self):
        f = getattr(self, "_cancel_function", None)
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
