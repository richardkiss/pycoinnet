"""
Some useful constructs for building asynchronous pipelines where data goes through multiple blocking stage
pipe, being transformed and queued at each stage of the pipeline.

At each stage, you can control the number of workers pulling data out, as well as the size and type of
the queue it puts the data into.
"""

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


def make_pipe(map_f, input_q=None, output_q=None, worker_count=None, loop=None):
    """
    Join two queues by a map function, so items are pulled from input_q, the asynch map function
    is applied, then the result is pushed into output_q.
    """

    output_q = output_q or asyncio.Queue()

    async def new_map_f(item):
        r = await map_f(item)
        await output_q.put(r)

    input_q = asyncmap(new_map_f, q=input_q, worker_count=worker_count, loop=loop)
    return input_q.put, output_q, input_q.cancel


def make_flattener(input_q=None, output_q=None, loop=None):
    """
    Join two queues by a flattening function, so iterables that are pulled from input_q are iterated and
    the resulting elements sequentially pushed into output_q.
    """

    output_q = output_q or asyncio.Queue()

    async def new_map_f(items):
        for item in items:
            await output_q.put(item)

    input_q = asyncmap(new_map_f, q=input_q, worker_count=1, loop=loop)
    return input_q.put, output_q, input_q.cancel


def make_pipeline(*args, final_q=None, loop=None):
    """
    Create a pipeline of queues. Function => q => function => q
    Returns the first function and the last queue.

    each arg is a dictionary with the following optional parameters:
    """

    # build the queues
    queues = []
    for d in args:
        q = d.get("input_q") or asyncio.Queue(maxsize=d.get("input_q_maxsize", 0))
        queues.append(q)
    queues.append(final_q or asyncio.Queue())

    # build the pipeline
    cancel_functions = []
    for _, d in enumerate(args):
        input_q, output_q = queues[_:_+2]
        map_f = d.get("map_f")
        flatten = d.get("flatten")

        if (flatten, map_f).count(None) != 1:
            raise ValueError("exactly one of map_f and flatten must be set")

        if map_f and not asyncio.iscoroutinefunction(map_f):
            raise ValueError("map_f must be an async coroutine")

        if flatten:
            in_f, out_q, cancel = make_flattener(input_q=input_q, output_q=output_q, loop=loop)
            cancel_functions.append(cancel)

        if map_f:
            in_f, out_q, cancel = make_pipe(
                map_f, input_q=input_q, output_q=output_q, worker_count=d.get("worker_count"), loop=loop)
            cancel_functions.append(cancel)

    def cancel():
        for f in cancel_functions:
            f()

    return queues[0].put, queues[-1], cancel
