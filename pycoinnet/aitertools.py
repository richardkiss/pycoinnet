import asyncio
import logging
import weakref


async def azip(*aiters):
    """
    async version of zip
    example:
        async for a, b, c in azip(aiter1, aiter2, aiter3):
            print(a, b, c)
    """
    anext_list = [_.__aiter__() for _ in aiters]
    while True:
        try:
            next_list = [await _.__anext__() for _ in anext_list]
        except StopAsyncIteration:
            break
        yield tuple(next_list)


async def iter_to_aiter(iter):
    """
    This converts a regular iterator to an async iterator
    """
    for _ in iter:
        yield _


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


class push_aiter_head:
    def __init__(self, head):
        self._head = head
        self._head.push_aiter_head = self

    def push(self, *items):
        if self._head.cancelled():
            raise ValueError("%s closed" % self)
        for item in items:
            new_head = asyncio.Future()
            new_head.push_aiter_head = self
            self._head.set_result((item, new_head))
            self._head = new_head

    def stop(self):
        head = self._head
        if not head.done():
            head.cancel()

    def is_stopping(self):
        self._skip_to_head()
        return self._head.cancelled()


class push_aiter:
    def __init__(self, tail=None, next_preflight=None):
        if tail is None:
            tail = asyncio.Future()
            push_aiter_head(tail)
        self._tail = tail
        self._next_preflight = next_preflight
        self._semaphore = asyncio.Semaphore()

    def head(self):
        return self._tail.push_aiter_head

    async def push(self, *items):
        return self._tail.push_aiter_head.push(*items)

    def push_nowait(self, *items):
        return self._tail.push_aiter_head.push(*items)

    def stop(self):
        return self._tail.push_aiter_head.stop()

    def __aiter__(self):
        return self

    def fork(self, is_active=True):
        next_preflight = self._next_preflight if is_active else None
        return self.__class__(tail=self._tail, next_preflight=next_preflight)

    async def __anext__(self):
        async with self._semaphore:
            if self._next_preflight:
                self._next_preflight(self)
            try:
                _, self._tail = await self._tail
                return _
            except asyncio.CancelledError:
                raise StopAsyncIteration

    def is_stopped(self):
        return self._tail.cancelled()

    def is_item_available(self):
        return self.is_len_at_least(1)

    def is_len_at_least(self, n):
        tail = self._tail
        while n > 0 and tail.done() and not tail.cancelled():
            _, tail = tail.result()
            n -= 1
        return n <= 0

    def __len__(self):
        breakpoint()
        count = 0
        tail = self._tail
        while tail.done() and not tail.cancelled():
            _, tail = head.result()
            count += 1
        return count


def aiter_forker(aiter):
    """
    Wrap an iterator with push_aiter. This can also be forked.
    """

    open_aiter = aiter.__aiter__()

    async def worker(open_aiter, pa):
        try:
            _ = await open_aiter.__anext__()
            await pa.push(_)
        except StopAsyncIteration:
            pa.stop()

    def make_kick():
        def kick(pa):
            if pa.head().task and not pa.head().task.done():
                return
            pa.head().task = asyncio.ensure_future(worker(open_aiter, pa))
        return kick

    pa = push_aiter(next_preflight=make_kick())
    pa.head().task = asyncio.ensure_future(worker(open_aiter, pa))
    return pa


async def join_aiters(aiter_of_aiters):
    """
    Takes an iterator of async iterators and pipe them into a single async iterator.

    This creates a task to monitor the main iterator, plus a task for each active
    iterator that has come out of the main iterator.
    """

    async def aiter_to_next_job(aiter):
        """
        Return items to add to stack, plus jobs to add to queue.
        """
        try:
            v = await aiter.__anext__()
            return [v], [asyncio.ensure_future(aiter_to_next_job(aiter))]
        except StopAsyncIteration:
            return [], []

    async def main_aiter_to_next_job(aiter_of_aiters):
        try:
            new_aiter = await aiter_of_aiters.__anext__()
            return [], [
                asyncio.ensure_future(aiter_to_next_job(new_aiter.__aiter__())),
                asyncio.ensure_future(main_aiter_to_next_job(aiter_of_aiters))]
        except StopAsyncIteration:
            return [], []

    jobs = set([main_aiter_to_next_job(aiter_of_aiters.__aiter__())])

    while jobs:
        done, jobs = await asyncio.wait(jobs, return_when=asyncio.FIRST_COMPLETED)
        for _ in done:
            new_items, new_jobs = await _
            for _ in new_items:
                yield _
            jobs.update(_ for _ in new_jobs)


async def map_aiter(map_f, aiter):
    """
    Take an async iterator and a map function, and apply the function
    to everything coming out of the iterator before passing it on.
    """
    if asyncio.iscoroutinefunction(map_f):
        _map_f = map_f
    else:
        async def _map_f(_):
            return map_f(_)

    async for _ in aiter:
        try:
            yield await _map_f(_)
        except Exception:
            logging.exception("unhandled mapping function %s worker exception on %s", map_f, _)


async def flatten_aiter(aiter):
    """
    Take an async iterator that returns lists and return the individual
    elements.
    """
    async for items in aiter:
        try:
            for _ in items:
                yield _
        except Exception:
            pass


async def map_filter_aiter(map_f, aiter):
    """
    In this case, the map_f must return a list, which will be flattened.
    You can filter items by excluding them from the list.
    Empty lists are okay.
    """
    if asyncio.iscoroutinefunction(map_f):
        _map_f = map_f
    else:
        async def _map_f(_):
            return map_f(_)

    async for _ in aiter:
        try:
            items = await _map_f(_)
            for _ in items:
                yield _
        except Exception:
            logging.exception("unhandled mapping function %s worker exception on %s", map_f, _)


def parallel_map_aiter(map_f, worker_count, aiter, q=None, maxsize=1):
    shared_aiter = aiter_forker(aiter)
    aiters = [map_aiter(map_f, shared_aiter) for _ in range(worker_count)]
    return join_aiters(iter_to_aiter(aiters))


def rated_aiter(rate_limiter, aiter):
    """
    Returns a pair: an iter along with a function that you can "push"
    integer values into.

    This is kind of like an electronic transistor, except discrete.
    """
    r_aiter = map_aiter(lambda x: x[0], azip(aiter, map_filter_aiter(range, rate_limiter)))
    return r_aiter
