import asyncio
import time
import unittest

from pycoinnet.pipeline import asyncmap, make_flattener, make_pipeline, make_pipe


class PipelineTest(unittest.TestCase):

    def test_pipeline(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def transformation_f(item):
            results.append(item)
            if len(results) == 3:
                event.set()

        in_f, q, cancel = make_pipe(transformation_f)

        async def go(in_f, q, cancel):
            await in_f(5)
            await in_f(4)
            await in_f(3)
            await event.wait()
            cancel()

        loop.run_until_complete(go(in_f, q, cancel))
        self.assertEqual(results, [5, 4, 3])

    def test_asyncmap(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def async_transformation_f(item):
            results.append(item)
            if len(results) == 3:
                event.set()

        def sync_transformation_f(item):
            results.append(item)
            if len(results) == 3:
                event.set()

        async def go(q):
            await q.put(5)
            await q.put(4)
            await q.put(3)
            await event.wait()
            q.cancel()

        loop.run_until_complete(go(asyncmap(async_transformation_f)))
        self.assertEqual(results, [5, 4, 3])

        results = []
        event.clear()
        loop.run_until_complete(go(asyncmap(async_transformation_f, worker_count=1)))
        self.assertEqual(results, [5, 4, 3])

        self.assertRaises(ValueError, lambda: asyncmap(sync_transformation_f))

    def test_make_flattener(self):
        loop = asyncio.get_event_loop()

        r = []

        async def go():
            in_f, out_q, cancel = make_flattener()
            await in_f([0, 1, 2, 3])
            for _ in range(3):
                r.append(await out_q.get())
            await in_f([4, 5, 6, 7])
            for _ in range(5):
                r.append(await out_q.get())
            cancel()

        loop.run_until_complete(go())
        self.assertEqual(r, list(range(8)))

    def test_make_pipe(self):
        loop = asyncio.get_event_loop()

        async def map_f(x):
            await asyncio.sleep(x / 10.0)
            return x * x

        r = []

        async def go():
            in_f, out_q, cancel = make_pipe(map_f)
            for _ in range(4):
                await in_f(_)
            for _ in range(3, 9):
                await in_f(_)
            for _ in range(10):
                r.append(await out_q.get())
            cancel()

        loop.run_until_complete(go())
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):
        loop = asyncio.get_event_loop()

        in_f, out_q, cancel = make_pipeline(
            dict(flatten=True),
            dict(flatten=True),
        )

        async def go(in_f, out_q, cancel):
            await in_f([
                (0, 0, 1, 0),
                (1, 1, 1, 1),
                (2, 0, 0, 1),
                (3, 1, 2, 0),
                (0, 0, 0, 7),
            ])
            r = []
            for _ in range(20):
                p = await out_q.get()
                r.append(p)
            cancel()
            self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0, 1, 3, 1, 2, 0, 0, 0, 0, 7])
        loop.run_until_complete(go(in_f, out_q, cancel))

    def test_make_delayed_pipeline(self):
        loop = asyncio.get_event_loop()

        def make_wait_index(idx):

            async def wait(item):
                await asyncio.sleep(item[idx] / 10.)
                return item

            return wait

        in_f, out_q, cancel = make_pipeline(
            dict(flatten=True),
            dict(map_f=make_wait_index(0)),
            dict(map_f=make_wait_index(1)),
            dict(map_f=make_wait_index(2)),
            dict(map_f=make_wait_index(3)),
        )

        TEST_CASE = [
            (0, 0, 0, 7),
            (5, 0, 0, 0),
            (0, 0, 1, 0),
            (1, 1, 1, 1),
            (2, 0, 0, 1),
            (3, 1, 2, 0),
        ]

        async def go(case, in_f, out_q):
            await in_f(case)
            r = []
            for _ in range(len(case)):
                p = await out_q.get()
                r.append(p)
            cancel()
            r1 = sorted(r, key=lambda x: sum(x))
            self.assertEqual(r, r1)
        loop.run_until_complete(go(TEST_CASE, in_f, out_q))
