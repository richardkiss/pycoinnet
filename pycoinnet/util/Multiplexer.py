
import asyncio

from weakref import WeakSet


class Multiplexer:
    def __init__(self, get_next_obj, loop=None):
        loop = loop or asyncio.get_event_loop()
        self.loop = loop
        self.get_next_obj = get_next_obj
        self.queue_set = WeakSet()
        self.event_queue_nonempty = asyncio.Event(loop=loop)

        @asyncio.coroutine
        def f():
            while True:
                item = yield from self.get_next_obj()
                if len(self.queue_set) == 0:
                    self.event_queue_nonempty.clear()
                    yield from self.event_queue_nonempty.wait()
                for q in self.queue_set:
                    try:
                        q.put_nowait(item)
                    except asyncio.QueueFull:
                        pass
        self.run_task = asyncio.Task(f(), loop=loop)

    def new_q(self, maxsize=0):
        q = asyncio.Queue(maxsize=maxsize, loop=self.loop)
        self.queue_set.add(q)
        self.event_queue_nonempty.set()
        return q.get

    def remove_q(self, q):
        # this is mostly for testing. It ensures that
        # the queue_set has discarded the given q.
        # It's difficult to ensure that the WeakSet
        # will release the unreferenced object otherwise.
        self.queue_set.discard(getattr(q, "__self__"))

    def cancel(self):
        self.run_task.cancel()
        self.queue_set.clear()
