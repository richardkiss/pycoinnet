import asyncio
import logging


class iter_to_aiter:
    """
    This converts a regular iterator to an async iterator
    """
    def __init__(self, iterator):
        self._input = iterator.__iter__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._input)
        except StopIteration:
            raise StopAsyncIteration

    def __repr__(self):
        return "<iter_to_aiter wrapping %s>" % self._input


def aiter_to_iter(aiter, loop=None):
    """
    Convert an async iterator to a regular iterator by invoking
    run_until_complete repeatedly.
    """
    if loop is None:
        loop = asyncio.get_event_loop()
    underlying_aiter = aiter.__aiter__()
    while True:
        try:
            _ = loop.run_until_complete(underlying_aiter.__anext__())
            yield _
        except StopAsyncIteration:
            break


class q_aiter:
    """
    Creates an async iterator that you can "push" items into.
    Call "stop" when no more items will be added to the queue, so the iterator
    knows to end.
    """
    def __init__(self, q=None, maxsize=1, full_callback=None):
        if q is None:
            q = asyncio.Queue(maxsize=maxsize)
        self._q = q
        self._stopping = asyncio.Future()
        self._stopped = False
        self._full_callback = full_callback

    def stop(self):
        """
        No more items will be added to the queue. Items in queue will be processed,
        then a StopAsyncIteration raised.
        """
        if not self._stopping.done():
            self._stopping.set_result(True)

    def q(self):
        return self._q

    async def push(self, item):
        if self._stopping.done():
            raise ValueError("%s closed" % self)
        if self._full_callback and self._q.full():
            self._full_callback(self, item)
        await self._q.put(item)

    def push_nowait(self, item):
        if self._stopping.done():
            raise ValueError("%s closed" % self)
        if self._full_callback and self._q.full():
            self._full_callback(self, item)
        self._q.put_nowait(item)

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._stopping.done():
                if self._stopped:
                    raise StopAsyncIteration
                try:
                    return self._q.get_nowait()
                except asyncio.queues.QueueEmpty:
                    self._stopped = True
                    raise StopAsyncIteration

            q_get = asyncio.ensure_future(self._q.get())
            done, pending = await asyncio.wait([self._stopping, q_get], return_when=asyncio.FIRST_COMPLETED)
            if q_get in done:
                return q_get.result()
            q_get.cancel()

    def __repr__(self):
        return "<q_aiter %s>" % self._q


class join_aiters(q_aiter):
    """
    Takes a list of async iterators and pipes them into a single async iterator.
    """
    def __init__(self, _aiter_of_aiters, q=None, maxsize=1):
        super(join_aiters, self).__init__(q=q, maxsize=maxsize)
        self._task_dict = {}
        self._aiter_of_aiters = _aiter_of_aiters
        self._task = asyncio.ensure_future(self._add_task())

    async def _add_task(self):
        async for aiter in self._aiter_of_aiters:
            self._task_dict[aiter] = asyncio.ensure_future(self._worker(aiter))
        for task in list(self._task_dict.values()):
            await task
        self.stop()

    async def _worker(self, aiter):
        async for _ in aiter:
            await self.push(_)
        del self._task_dict[aiter]


def sharable_aiter(aiter):
    return join_aiters(iter_to_aiter([aiter]), maxsize=1)


class map_aiter:
    """
    Take an async iterator and a map function, and apply the function
    to everything coming out of the iterator before passing it on.
    """
    def __init__(self, map_f, aiter):
        self._aiter = aiter.__aiter__()
        self._map_f = map_f

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            n = await self._aiter.__anext__()
            try:
                return await self._map_f(n)
            except Exception:
                logging.exception("unhandled mapping function %s worker exception on %s", self._map_f, n)

    def __repr__(self):
        return "<map_aiter wrapping %s>" % self._aiter


class flatten_aiter(q_aiter):
    """
    Take an async iterator and a map function, and apply the function
    to everything coming out of the iterator before passing it on.
    """
    def __init__(self, aiter, q=None, maxsize=1):
        super(flatten_aiter, self).__init__(q=q, maxsize=maxsize)
        self._aiter = aiter
        self._task = asyncio.ensure_future(self._worker())

    async def _worker(self):
        async for items in self._aiter:
            if items:
                for _ in items:
                    await self.push(_)
        self.stop()

    def __repr__(self):
        return "<flatten_aiter wrapping %s>" % self._aiter


def map_filter_aiter(map_f, aiter):
    """
    In this case, the map_f must return a list, which will be flattened.
    You can filter items by returning an empty list.
    """
    return flatten_aiter(map_aiter(map_f, aiter))



def parallel_map_aiter(map_f, worker_count, aiter, maxsize=1):
    shared_aiter = sharable_aiter(aiter)
    aiters = [map_aiter(map_f, shared_aiter) for _ in range(worker_count)]
    return join_aiters(iter_to_aiter(aiters), maxsize=maxsize)
