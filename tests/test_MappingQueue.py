import asyncio
import time
import unittest

from pycoinnet.MappingQueue import MappingQueue


class MappingQueueTest(unittest.TestCase):

    def test_pipeline(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def transformation_f(item):
            results.append(item)
            if len(results) == 3:
                event.set()

        q = MappingQueue({"map_f": transformation_f})

        async def go(q):
            await q.put(5)
            await q.put(4)
            await q.put(3)
            await event.wait()
            q.cancel()

        loop.run_until_complete(go(q))
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

        loop.run_until_complete(go(MappingQueue({"map_f": async_transformation_f})))
        self.assertEqual(results, [5, 4, 3])

        results = []
        event.clear()
        loop.run_until_complete(go(MappingQueue({"map_f": async_transformation_f, "worker_count": 1})))
        self.assertEqual(results, [5, 4, 3])

        self.assertRaises(ValueError, lambda: MappingQueue({"map_f": sync_transformation_f, "worker_count": 1}))

    def test_make_flattener(self):
        loop = asyncio.get_event_loop()

        r = []

        async def go(q):
            await q.put([0, 1, 2, 3])
            for _ in range(3):
                r.append(await q.get())
            await q.put([4, 5, 6, 7])
            for _ in range(5):
                r.append(await q.get())
            q.cancel()

        loop.run_until_complete(go(MappingQueue({"flatten": True})))
        self.assertEqual(r, list(range(8)))

    def test_make_pipe(self):
        loop = asyncio.get_event_loop()

        async def map_f(x):
            await asyncio.sleep(x / 10.0)
            return x * x

        r = []

        async def go(q):
            for _ in range(4):
                await q.put(_)
            for _ in range(3, 9):
                await q.put(_)
            for _ in range(10):
                r.append(await q.get())
            q.cancel()

        loop.run_until_complete(go(MappingQueue(dict(map_f=map_f))))
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):
        loop = asyncio.get_event_loop()

        q = MappingQueue(
            dict(flatten=True),
            dict(flatten=True),
        )

        async def go(q):
            await q.put([
                (0, 0, 1, 0),
                (1, 1, 1, 1),
                (2, 0, 0, 1),
                (3, 1, 2, 0),
                (0, 0, 0, 7),
            ])
            r = []
            for _ in range(20):
                p = await q.get()
                r.append(p)
            q.cancel()
            self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0, 1, 3, 1, 2, 0, 0, 0, 0, 7])
        loop.run_until_complete(go(q))

    def test_make_delayed_pipeline(self):
        loop = asyncio.get_event_loop()

        def make_wait_index(idx):

            async def wait(item):
                await asyncio.sleep(item[idx] / 10.)
                return item

            return wait

        q = MappingQueue(
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

        async def go(case, q):
            await q.put(case)
            r = []
            for _ in range(len(case)):
                p = await q.get()
                r.append(p)
            q.cancel()
            r1 = sorted(r, key=lambda x: sum(x))
            self.assertEqual(r, r1)
        loop.run_until_complete(go(TEST_CASE, q))
