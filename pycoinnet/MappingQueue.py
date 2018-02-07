import asyncio


def asyncmap(map_f, q=None, worker_count=None, loop=None):
    """
    This function is an asynchronous mapping function. It pulls items out of a queue,
    then applies an asynchronous function.

    q: the queue to pull from. If unset, a Queue of unlimited size is created
    worker_count: the number of tasks created. If none, each new item gets its own new task.
        Otherwise, tasks are recycled
    map_f: the function called when an item is pulled from the queue. Must be async.
    """

    q = q or asyncio.Queue()
    loop = loop or asyncio.get_event_loop()

    if map_f and not asyncio.iscoroutinefunction(map_f):
        raise ValueError("map_f must be an async coroutine")

    tasks = set()

    def cancel():
        for task in tasks:
            if not task.done():
                task.cancel()

    async def _worker(q):
        while True:
            item = await q.get()
            try:
                await map_f(item)
            except Exception:
                pass

    async def _boss(q, tasks):
        while True:
            item = await q.get()
            task = loop.create_task(map_f(item))
            tasks.add(task)
            task.add_done_callback(lambda r: tasks.discard(task))

    if worker_count:
        for _ in range(worker_count):
            tasks.add(loop.create_task(_worker(q)))
    else:
        boss_task = loop.create_task(_boss(q, tasks))
        tasks.add(boss_task)

    q.cancel = cancel
    return q


class MappingQueue:
    def __init__(self, *args, final_q=None, loop=None):
        """
        Create a pipeline of queues. q => function => q => function => q => ... => final_q

        Values get "async put" into the queue, and come out some time later after processing
        with "async get".

        each arg is a dictionary with the following optional parameters:
        input_q: the Queue subclass
        input_q_maxsize: the maxsize of the Queue
        worker_count: maximum number of tasks pulling from the queue, or None for unlimited
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

        def make_callback_function(callback_f, output_q):

            async def callback(item):
                await callback_f(item, output_q)

            return callback

        # build the pipeline
        cancel_functions = []

        for q in queues:
            cancel = getattr(q, "cancel", None)
            if callable(cancel):
                cancel_functions.append(cancel)

        for _, d in enumerate(args):
            input_q, output_q = queues[_:_+2]
            callback_f = d.get("callback_f")
            worker_count = d.get("worker_count")
            if not asyncio.iscoroutinefunction(callback_f):
                raise ValueError("callback_f must be an async coroutine")

            callback = make_callback_function(callback_f, output_q)

            input_q = asyncmap(map_f=callback, q=input_q, worker_count=worker_count, loop=loop)
            cancel_functions.append(input_q.cancel)

        def cancel():
            for f in cancel_functions:
                f()

        self._in_q = queues[0]
        self._out_q = queues[-1]
        self._cancel_function = cancel

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
