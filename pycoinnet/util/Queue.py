
import asyncio.queues
import logging

class Queue(asyncio.queues.Queue):
    """
    We add a "get_all" method here.
    """
    @asyncio.coroutine
    def get_all(self):
        items = []
        if self.empty():
            item = yield from self.get()
            items.append(item)
        try:
            for i in range(self.qsize()):
                items.append(self.get_nowait())
        except asyncio.queues.Empty:
            logging.error("get_nowait failed unexpectedly")
        return items
