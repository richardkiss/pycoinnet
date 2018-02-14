import asyncio
import unittest

from pycoinnet.MappingQueue import MappingQueue


async def flatten_callback(items, output_q):
    for item in items:
        await output_q.put(item)


class MappingQueueTest(unittest.TestCase):

    def test_pipeline(self):
        loop = asyncio.get_event_loop()
        results = []
        event = asyncio.Event()

        async def callback_f(item, q):
            await q.put(item)
            results.append(item)
            if len(results) == 3:
                event.set()

        q = MappingQueue({"callback_f": callback_f})

        async def go(q):
            await q.put(5)
            await q.put(4)
            await q.put(3)
            await event.wait()
            await q.cancel()

        loop.run_until_complete(go(q))
        self.assertEqual(results, [5, 4, 3])

    def test_asyncmap(self):
        results = []

        async def async_transformation_f(item, q):
            await q.put(item)
            results.append(item)

        def sync_transformation_f(item, q):
            q.put_nowait(item)
            results.append(item)

        async def go(q):
            await q.put(5)
            await q.put(4)
            await q.put(3)
            await q.done()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go(MappingQueue({"callback_f": async_transformation_f, "worker_count": 1})))
        self.assertEqual(results, [5, 4, 3])

        results = []

        loop.run_until_complete(go(MappingQueue({"callback_f": async_transformation_f, "worker_count": 1})))
        self.assertEqual(results, [5, 4, 3])

        self.assertRaises(ValueError, lambda: MappingQueue(
            {"callback_f": sync_transformation_f, "worker_count": 1}))

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
            await q.done()

        loop.run_until_complete(go(MappingQueue({"callback_f": flatten_callback})))
        self.assertEqual(r, list(range(8)))

    def test_make_pipe(self):
        loop = asyncio.get_event_loop()

        async def map_f(x, q):
            await asyncio.sleep(x / 10.0)
            await q.put(x * x)

        r = []

        async def go(q):
            for _ in range(4):
                await q.put(_)
            for _ in range(3, 9):
                await q.put(_)
            for _ in range(10):
                r.append(await q.get())
            await q.cancel()

        loop.run_until_complete(go(MappingQueue(dict(callback_f=map_f))))
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):
        loop = asyncio.get_event_loop()

        q = MappingQueue(
            dict(callback_f=flatten_callback),
            dict(callback_f=flatten_callback),
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
            await q.cancel()
            self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0, 1, 3, 1, 2, 0, 0, 0, 0, 7])
        loop.run_until_complete(go(q))

    def test_make_delayed_pipeline(self):
        loop = asyncio.get_event_loop()

        def make_wait_index(idx):

            async def wait(item, q):
                await asyncio.sleep(item[idx] / 10.)
                await q.put(item)

            return wait

        q = MappingQueue(
            dict(callback_f=flatten_callback),
            dict(callback_f=make_wait_index(0), worker_count=10),
            dict(callback_f=make_wait_index(1), worker_count=10),
            dict(callback_f=make_wait_index(2), worker_count=10),
            dict(callback_f=make_wait_index(3), worker_count=10),
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
            await q.cancel()
            r1 = sorted(r, key=lambda x: sum(x))
            self.assertEqual(r, r1)
        loop.run_until_complete(go(TEST_CASE, q))

    def test_filter_pipeline(self):
        loop = asyncio.get_event_loop()

        async def filter(item_list_of_lists, q):
            for l1 in item_list_of_lists:
                for item in l1:
                    if item != 0:
                        await q.put(item)

        q = MappingQueue(
            dict(callback_f=filter),
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
            for _ in range(12):
                p = await q.get()
                r.append(p)
            await q.cancel()
            r1 = [7, 5, 1, 1, 1, 1, 1, 2, 1, 3, 1, 2]
            self.assertEqual(r, r1)
        loop.run_until_complete(go(TEST_CASE, q))
