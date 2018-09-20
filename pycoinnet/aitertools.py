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
    anext_tuple = tuple([_.__aiter__() for _ in aiters])
    while True:
        try:
            next_tuple = tuple([await _.__anext__() for _ in anext_tuple])
        except StopAsyncIteration:
            break
        yield next_tuple


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
    """
    An asynchronous iterator based on a linked-list.
    Data goes in the head via "push".
    Allows peeking to determine how many elements are ready.
    Can be copied very cheaply by copying the tail.
    Has an "preflight" that is called whenever __anext__ is called.
    The __anext__ method is wrapped with a semaphore so multiple
    tasks can use the same iterator and each will only get an output once.
    """
    def __init__(self, tail=None, next_preflight=None):
        if tail is None:
            tail = asyncio.Future()
            push_aiter_head(tail)
        self._tail = tail
        self._next_preflight = next_preflight
        self._semaphore = asyncio.Semaphore()

    def head(self):
        return self._tail.push_aiter_head

    def push(self, *items):
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

    def available_iter(self):
        tail = self._tail
        try:
            while tail.done():
                _, tail = tail.result()
                yield _
        except asyncio.CancelledError:
            pass

    def is_stopped(self):
        return self._tail.cancelled()

    def is_item_available(self):
        return self.is_len_at_least(1)

    def is_len_at_least(self, n):
        for _, item in enumerate(self.available_iter()):
            if _+1 >= n:
                return True
        return False

    def __len__(self):
        return sum(1 for _ in self.available_iter())


def aiter_forker(aiter):
    """
    Wrap an iterator with push_aiter. This can also be forked.
    """

    open_aiter = aiter.__aiter__()

    async def worker(open_aiter, pa):
        try:
            _ = await open_aiter.__anext__()
            if not pa.is_stopped():
                pa.push(_)
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


async def active_aiter(aiter):
    """
    Wrap an aiter with an active puller that yanks out the items
    and puts them into a push_q.
    """
    q = push_aiter()
    async def _pull_task(aiter):
        async for _ in aiter:
            q.push(_)
        q.stop()

    task = asyncio.ensure_future(_pull_task(aiter))

    async for _ in q:
        yield _
    await task


class sharable_aiter:
    def __init__(self, aiter):
        self._opened_aiter = aiter.__aiter__()
        self._semaphore = asyncio.Semaphore()

    def __aiter__(self):
        return self

    async def __anext__(self):
        async with self._semaphore:
            return await self._opened_aiter.__anext__()


class gated_aiter:
    """
    Returns a pair: an iter along with a function that you can "push"
    integer values into. When a number is pushed, that many items are
    allowed out through the gate.

    This is kind of like a discrete version of an electronic transistor.
    """
    def __init__(self, aiter):
        self._gate = push_aiter()
        self._open_aiter = active_aiter(azip(aiter, map_filter_aiter(range, self._gate))).__aiter__()
        self._semaphore = asyncio.Semaphore()

    def __aiter__(self):
        return self

    async def __anext__(self):
        async with self._semaphore:
            return (await self._open_aiter.__anext__())[0]

    def push(self, count):
        if not self._gate.is_stopped():
            self._gate.push(count)

    def stop(self):
        self._gate.stop()


async def preload_aiter(preload_size, aiter):
    """
    This aiter wraps around another aiter, and forces a preloaded
    buffer of the given size.
    """

    gate = gated_aiter(aiter)
    gate.push(preload_size)
    async for _ in gate:
        yield _
        gate.push(1)
    gate.stop()


class stoppable_aiter:
    def __init__(self, aiter):
        self._open_aiter = aiter.__aiter__()
        self._is_stopping = False
        self._semaphore = asyncio.Semaphore()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._is_stopping:
            raise StopAsyncIteration
        async with self._semaphore:
            return await self._open_aiter.__anext__()

    def stop(self):
        self._is_stopping = True
