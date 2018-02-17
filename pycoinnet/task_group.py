import asyncio
import logging


class TaskGroup:
    def __init__(self, repeated_f, worker_count=1, done_callback=None, loop=None):
        loop = loop or asyncio.get_event_loop()
        if not asyncio.iscoroutinefunction(repeated_f):
            raise ValueError("repeated_f must be an async coroutine")

        self._repeated_f = repeated_f
        self._is_stopping = asyncio.Event()
        self._is_immediate = False
        self._task = loop.create_task(self._run(worker_count, loop))
        if done_callback:
            self._task.add_done_callback(done_callback)

    async def _run(self, worker_count, loop):
        self._tasks = set()
        for _ in range(worker_count):
            self._tasks.add(loop.create_task(self._worker()))
        await asyncio.wait(self._tasks)

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
