import asyncio
import unittest

from pycoinnet.aitertools import q_aiter, flatten_aiter, map_aiter, parallel_map_aiter


async def flatten_callback(items, output_q):
    for item in items:
        await output_q.put(item)


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


async def get_n(aiter, n=0):
    """
    Get n items.
    """
    r = []
    count = 0
    async for _ in aiter:
        r.append(_)
        count += 1
        if count >= n and n != 0:
            break
    return r


class test_aitertools(unittest.TestCase):

    def test_q_aiter(self):

        async def go(q, results):
            await q.put(5)
            q.put_nowait(4)
            await q.put(3)
            q.stop()
            await q.put(2)
            async for _ in q:
                results.append(_)

        q = q_aiter()
        results = []
        run(go(q, results))
        self.assertEqual(results, [5, 4, 3])

    def test_asyncmap(self):

        def make_async_transformation_f(results):
            async def async_transformation_f(item):
                results.append(item)
                return item
            return async_transformation_f

        async def go(q):
            await q.put(5)
            await q.put(4)
            await q.put(3)
            q.stop()

        results = []
        q = q_aiter()
        aiter = map_aiter(make_async_transformation_f(results), q)
        run(go(q))
        self.assertEqual(results, [])
        r = run(get_n(aiter))
        self.assertEqual(r, [5, 4, 3])
        self.assertEqual(results, [5, 4, 3])

    def test_flatten_aiter(self):
        async def go():
            q = q_aiter()
            fi = flatten_aiter(q)
            r = []
            await q.put([0, 1, 2, 3])

            r.extend(await get_n(fi, 3))
            await q.put([4, 5, 6, 7])
            r.extend(await get_n(fi, 5))
            q.stop()
            r.extend(await get_n(fi))
            return r

        r = run(go())
        self.assertEqual(r, list(range(8)))

    def test_make_pipe(self):
        async def map_f(x):
            await asyncio.sleep(x / 10.0)
            return x * x

        async def go():
            q = q_aiter()
            aiter = map_aiter(map_f, q)
            for _ in range(4):
                await q.put(_)
            for _ in range(3, 9):
                await q.put(_)
            r = await get_n(aiter, 10)
            q.stop()
            r.extend(await get_n(aiter))
            return r

        r = run(go())
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):

        async def go():
            q = q_aiter()
            aiter = flatten_aiter(flatten_aiter(q))
            await q.put([
                (0, 0, 1, 0),
                (1, 1, 1, 1),
                (2, 0, 0, 1),
                (3, 1, 2, 0),
                (0, 0, 0, 7),
            ])
            r = await get_n(aiter, 11)
            r.extend(await get_n(aiter, 8))
            q.stop()
            async for _ in aiter:
                r.append(_)
            return r

        r = run(go())
        self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0, 1, 3, 1, 2, 0, 0, 0, 0, 7])

    def test_make_delayed_pipeline(self):
        def make_wait_index(idx):

            async def wait(item, q):
                await asyncio.sleep(item[idx] / 10.)
                await q.put(item)

            return wait

        TEST_CASE = [
            (0, 0, 0, 7),
            (5, 0, 0, 0),
            (0, 0, 1, 0),
            (1, 1, 1, 1),
            (2, 0, 0, 1),
            (3, 1, 2, 0),
        ]

        async def go(case):
            q = q_aiter()
            aiter = flatten_aiter(
                parallel_map_aiter(make_wait_index(0), 10,
                    parallel_map_aiter(make_wait_index(1), 10,
                        parallel_map_aiter(make_wait_index(2), 10,
                            parallel_map_aiter(make_wait_index(3), 10, q
            )))))
            await q.put(case)
            q.stop()
            r = [_ async for _ in aiter]
            return r
        r = run(go(TEST_CASE))
        r1 = sorted(r, key=lambda x: sum(x))
        self.assertEqual(r, r1)

    def test_filter_pipeline(self):
        async def filter(item_list_of_lists):
            r = []
            for l1 in item_list_of_lists:
                for item in l1:
                    if item != 0:
                        r.append(item)
            return r

        TEST_CASE = [
            (0, 0, 0, 7),
            (5, 0, 0, 0),
            (0, 0, 1, 0),
            (1, 1, 1, 1),
            (2, 0, 0, 1),
            (3, 1, 2, 0),
        ]

        async def go(case):
            q = q_aiter()
            aiter = flatten_aiter(map_aiter(filter, q))
            await q.put(case)
            r = await get_n(aiter, 12)
            q.stop()
            return r
        r = run(go(TEST_CASE))
        r1 = [7, 5, 1, 1, 1, 1, 1, 2, 1, 3, 1, 2]
        self.assertEqual(r, r1)
