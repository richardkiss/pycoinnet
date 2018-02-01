import asyncio
import time
import unittest

from pycoinnet.pipeline import pipeline


class PipelineTest(unittest.TestCase):

    def test_pipeline(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def transformation_f(item):
            await asyncio.sleep(item * 0.1)
            results.append(item)
            if len(results) == 3:
                event.set()

        q = pipeline(transformation_f)
        q.put_nowait(5)
        q.put_nowait(4)
        q.put_nowait(3)

        async def go():
            await event.wait()
            await q.close()

        loop.run_until_complete(go())
        self.assertEqual(results, [3, 4, 5])

    def test_pipeline_None(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def transformation_f(item):
            await asyncio.sleep(item)
            results.append(item)

        q = pipeline(transformation_f)
        q.put_nowait(3)
        q.put_nowait(4)
        q.put_nowait(None)

        async def go():
            now = time.time()
            await q.close()
            delay = time.time() - now
            print(delay)
            assert delay >= 4.0

        loop.run_until_complete(go())
