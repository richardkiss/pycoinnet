import asyncio
import unittest

from pycoinnet.aitertools import (
    azip, aiter_forker, flatten_aiter, iter_to_aiter,
    q_aiter, map_aiter, parallel_map_aiter, join_aiters, rated_aiter
)


async def flatten_callback(items, output_q):
    for item in items:
        await output_q.push(item)


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
            await q.push(5)
            q.push_nowait(4)
            await q.push(3)
            q.stop()
            self.assertRaises(ValueError, lambda: run(q.push_nowait(2)))
            async for _ in q:
                results.append(_)

        q = q_aiter(maxsize=0)
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
            await q.push(5)
            await q.push(4)
            await q.push(3)
            q.stop()

        results = []
        q = q_aiter(maxsize=0)
        aiter = map_aiter(make_async_transformation_f(results), q)
        run(go(q))
        self.assertEqual(results, [])
        r = run(get_n(aiter))
        self.assertEqual(r, [5, 4, 3])
        self.assertEqual(results, [5, 4, 3])

    def test_flatten_aiter(self):
        async def go():
            q = q_aiter(maxsize=0)
            fi = flatten_aiter(q)
            r = []
            await q.push([0, 1, 2, 3])

            r.extend(await get_n(fi, 3))
            await q.push([4, 5, 6, 7])
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
            q = q_aiter(maxsize=0)
            aiter = map_aiter(map_f, q)
            for _ in range(4):
                await q.push(_)
            for _ in range(3, 9):
                await q.push(_)
            r = await get_n(aiter, 10)
            q.stop()
            r.extend(await get_n(aiter))
            return r

        r = run(go())
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):

        async def go():
            q = q_aiter(maxsize=0)
            aiter = flatten_aiter(flatten_aiter(q))
            await q.push([
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

    def test_join_aiters(self):
        int_vals = [1, 2, 3, 4]
        str_vals = "abcdefg"

        async def go(list_of_lists):
            iter_of_aiters = [iter_to_aiter(_) for _ in list_of_lists]
            r = []
            async for _ in join_aiters(iter_to_aiter(iter_of_aiters)):
                r.append(_)
            return r

        r = run(go([int_vals, str_vals]))
        r1 = [_ for _ in r if isinstance(_, int)]
        r2 = [_ for _ in r if isinstance(_, str)]
        self.assertEqual(r1, int_vals)
        self.assertEqual(r2, list(str_vals))

    def test_make_delayed_pipeline(self):
        def make_wait_index(idx):

            async def wait(item):
                await asyncio.sleep(item[idx] / 10.)
                return item

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
            q = q_aiter(maxsize=0)
            aiter = parallel_map_aiter(make_wait_index(0), 10,
                        parallel_map_aiter(make_wait_index(1), 10,
                            parallel_map_aiter(make_wait_index(2), 10,
                                parallel_map_aiter(make_wait_index(3), 10, q
            ))))
            await q.push(*case)
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
            q = q_aiter(maxsize=0)
            aiter = flatten_aiter(map_aiter(filter, q))
            await q.push(case)
            r = await get_n(aiter, 12)
            q.stop()
            return r
        r = run(go(TEST_CASE))
        r1 = [7, 5, 1, 1, 1, 1, 1, 2, 1, 3, 1, 2]
        self.assertEqual(r, r1)

    def test_aiter_forker(self):

        async def go():
            q = q_aiter(maxsize=0)
            forker = aiter_forker(q)
            await q.push(1, 2, 3, 4, 5)
            r0 = await get_n(forker, 3)
            f2 = forker.new_fork()
            await q.push(*range(7, 14))
            q.stop()
            r1 = await get_n(forker)
            r2 = await get_n(f2)
            return r0, r1, r2
        r0, r1, r2 = run(go())
        self.assertEqual(r0, [1, 2, 3])
        self.assertEqual(r1, [4, 5, 7, 8, 9, 10, 11, 12, 13])
        self.assertEqual(r2, [4, 5, 7, 8, 9, 10, 11, 12, 13])

    def test_aiter_forker_multiple_active(self):
        """
        Multiple forks of an aiter_forker both asking for empty q information
        at the same time. Make sure the second one doesn't block.
        """

        async def go():
            q = q_aiter(maxsize=0)
            forker = aiter_forker(q)
            fork_1 = forker.new_fork(is_active=True)
            fork_2 = forker.new_fork(is_active=True)
            f1 = asyncio.ensure_future(get_n(fork_1, 1))
            f2 = asyncio.ensure_future(get_n(fork_2, 1))
            await asyncio.wait([f1, f2], timeout=0.1)
            self.assertFalse(f1.done())
            self.assertFalse(f2.done())
            await q.push(1)
            await asyncio.wait([f1, f2], timeout=0.1)
            self.assertTrue(f1.done())
            self.assertTrue(f2.done())
            r1 = await f1
            r2 = await f2
            self.assertEqual(r1, [1])
            self.assertEqual(r2, [1])

        run(go())

    def test_azip(self):
        i1 = ("abcdefgh")
        i2 = list(range(20))
        i3 = list(str(_) for _ in range(20))
        ai1 = iter_to_aiter(i1)
        ai2 = iter_to_aiter(i2)
        ai3 = iter_to_aiter(i3)
        ai = azip(ai1, ai2, ai3)
        r = run(get_n(ai))
        self.assertEqual(r, list(zip(i1, i2, i3)))

    def test_rated_aiter(self):
        i1 = list(range(30))
        ai = iter_to_aiter(i1)
        rate_limiter = q_aiter()
        aiter = rated_aiter(rate_limiter, ai)
        rate_limiter.push_nowait(9)
        r = run(get_n(aiter, 3))
        r.extend(run(get_n(aiter, 4)))
        rate_limiter.push_nowait(11)
        rate_limiter.stop()
        r.extend(run(get_n(aiter)))
        self.assertEqual(r, list(range(20)))