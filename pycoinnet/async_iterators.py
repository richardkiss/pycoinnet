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


class stoppable_q:
    """
    Creates an async iterator attached to the backside of a queue.
    """
    def __init__(self, q=None, maxsize=0):
        if q is None:
            q = asyncio.Queue(maxsize=maxsize)
        self._q = q
        self._is_done = asyncio.Future()
        self._stopped = False

    def stop(self):
        """
        No more items will be added to the queue. Items in queue will be processed,
        then a StopAsyncIteration raised.
        """
        if not self._is_done.done():
            self._is_done.set_result(True)

    def q(self):
        return self._q

    def __aiter__(self):
        return self

    async def put(self, item):
        await self._q.put(item)

    async def __anext__(self):
        while True:
            if self._is_done.done():
                if self._stopped:
                    raise StopAsyncIteration
                try:
                    return self._q.get_nowait()
                except asyncio.queues.QueueEmpty:
                    self._stopped = True
                    raise StopAsyncIteration

            q_get = asyncio.ensure_future(self._q.get())
            done, pending = await asyncio.wait([self._is_done, q_get], return_when=asyncio.FIRST_COMPLETED)
            if q_get in done:
                v = q_get.result()
                return v
            q_get.cancel()

    def __repr__(self):
        return "<stoppable_q %s>" % self._q


class join_aiters(stoppable_q):
    """
    Takes a list of async iterators and pipes them into a single async iterator.
    """
    def __init__(self, *aiters, q=None, maxsize=0):
        super(join_aiters, self).__init__(q=q, maxsize=maxsize)
        self._task = asyncio.ensure_future(self._monitor_task(
            asyncio.gather(*[asyncio.ensure_future(self._worker(_)) for _ in aiters])))

    async def _worker(self, aiter):
        async for _ in aiter:
            await self._q.put(_)

    async def _monitor_task(self, task):
        await task
        self.stop()


def sharable_aiter(aiter):
    return join_aiters(aiter, maxsize=1)


class map_aiter:
    """
    Take an async iterator and a map function, and apply the function
    to everything coming out of the iterator before passing it on.
    """
    def __init__(self, aiter, map_f):
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


class flatten_aiter(stoppable_q):
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
                    await self._q.put(_)
        self.stop()

    def __repr__(self):
        return "<flatten_aiter wrapping %s>" % self._aiter


def parallel_map_aiter(aiter, map_f, worker_count=1, maxsize=1):
    shared_aiter = sharable_aiter(aiter)
    aiters = [map_aiter(shared_aiter, map_f) for _ in range(worker_count)]
    return join_aiters(*aiters, maxsize=maxsize)
