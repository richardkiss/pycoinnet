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
    

def asyncmap(map_f, q=None, done_callback_f=None, worker_count=1, loop=None):
    """
    This function is an asynchronous mapping function. It pulls items out of a queue,
    then applies an asynchronous function.

    q: the queue to pull from. If unset, a Queue of unlimited size is created
    worker_count: the number of tasks created
    map_f: the function called when an item is pulled from the queue. Must be async.
    """

    q = q or asyncio.Queue()
    loop = loop or asyncio.get_event_loop()

    if map_f and not asyncio.iscoroutinefunction(map_f):
        raise ValueError("map_f must be an async coroutine")

    if done_callback_f and not asyncio.iscoroutinefunction(done_callback_f):
        raise ValueError("done_callback_f must be an async coroutine")

    _done_event = asyncio.Event()

    async def _worker(q):
        is_done = asyncio.ensure_future(_done_event.wait())
        while True:
            q_get = asyncio.ensure_future(q.get())
            done, pending = await asyncio.wait([q_get, is_done], return_when=asyncio.FIRST_COMPLETED)

            if q_get not in done:
                break

            item = await q_get
            try:
                await map_f(item)
            except Exception:
                pass
            q.task_done()

        await q.join()
        print("worker exiting")

    tasks = set()
    for _ in range(worker_count):
        tasks.add(loop.create_task(_worker(q)))

    async def done():
        _done_event.set()
        await asyncio.wait(tasks)
        import pdb; pdb.set_trace()
        if done_callback_f:
            import pdb; pdb.set_trace()
            await done_callback_f()
            import pdb; pdb.set_trace()
        import pdb; pdb.set_trace()

    q.done = done
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

        def make_callback_function(callback_f, output_q):

            async def callback(item):
                await callback_f(item, output_q)

            return callback

        prior_done_f = None
        for _, d in enumerate(args):
            input_q, output_q = queues[_:_+2]
            callback_f = d.get("callback_f")
            worker_count = d.get("worker_count", 1)
            if not asyncio.iscoroutinefunction(callback_f):
                raise ValueError("callback_f must be an async coroutine")

            callback = make_callback_function(callback_f, output_q)

            input_q = asyncmap(
                map_f=callback, q=input_q, worker_count=worker_count, done_callback_f=prior_done_f, loop=loop)
            prior_done_f = input_q.done

        self._loop = loop or asyncio.get_event_loop()
        self._in_q = queues[0]
        self._out_q = queues[-1]
        self._done_function = prior_done_f

    async def done(self):
        if getattr(self, "_done_function", None):
            await self._done_function()
            self._done_function = None

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
