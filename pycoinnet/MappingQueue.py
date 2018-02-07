import asyncio

from pycoinnet.pipeline import make_pipeline


class MappingQueue(asyncio.Queue):
    def __init__(self, *args, final_q=None, loop=None):
        """
        Create a pipeline of queues. Function => q => function => q
        Returns the first function and the last queue.

        each arg is a dictionary with the following optional parameters:
        """
        put_f, out_q, cancel = make_pipeline(*args, final_q=final_q, loop=loop)
        self._in_q = put_f.__self__
        self._out_q = out_q
        self._cancel_functions = set([cancel])
        self._is_running = True

    def cancel(self):
        if self._is_running:
            for f in self._cancel_functions:
                f()
            self._is_running = False

    def __del__(self):
        if getattr(self, "_is_running", None):
            self.cancel()

    async def put(self, item):
        await self._in_q.put(item)

    async def get(self):
        return (await self._out_q.get())
