import asyncio
import unittest

from pycoinnet.aitertools import (
    azip, aiter_forker, flatten_aiter, iter_to_aiter, push_aiter,
    map_aiter, parallel_map_aiter, join_aiters, gated_aiter, map_filter_aiter, preload_aiter
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

    def test_push_aiter(self):
        q = push_aiter()
        self.assertEqual(len(q), 0)
        q.push(5, 4)
        self.assertEqual(len(q), 2)
        q.push(3)
        self.assertEqual(len(q), 3)
        q.stop()
        self.assertRaises(ValueError, lambda: q.push(2))
        results = list(q.available_iter())
        self.assertEqual(results, [5, 4, 3])
        results = run(get_n(q))
        self.assertEqual(results, [5, 4, 3])

    def test_asyncmap(self):

        def make_async_transformation_f(results):
            async def async_transformation_f(item):
                results.append(item)
                return item + 1
            return async_transformation_f

        results = []
        q = push_aiter()
        q.push(5, 4, 3)
        q.stop()
        r = list(q.available_iter())
        self.assertEqual(r, [5, 4, 3])
        aiter = map_aiter(make_async_transformation_f(results), q)
        r = run(get_n(aiter))
        self.assertEqual(r, [6, 5, 4])
        self.assertEqual(results, [5, 4, 3])

    def test_syncmap(self):

        def make_sync_transformation_f(results):
            def sync_transformation_f(item):
                results.append(item)
                return item + 1
            return sync_transformation_f

        results = []
        q = push_aiter()
        q.push(5, 4, 3)
        q.stop()
        r = list(q.available_iter())
        self.assertEqual(r, [5, 4, 3])
        aiter = map_aiter(make_sync_transformation_f(results), q)
        r = run(get_n(aiter))
        self.assertEqual(r, [6, 5, 4])
        self.assertEqual(results, [5, 4, 3])

    def test_flatten_aiter(self):
        q = push_aiter()
        fi = flatten_aiter(q)
        r = []
        q.push([0, 1, 2, 3])
        r.extend(run(get_n(fi, 3)))
        q.push([4, 5, 6, 7])
        r.extend(run(get_n(fi, 5)))
        q.stop()
        r.extend(run(get_n(fi)))
        self.assertEqual(r, list(range(8)))

    def test_make_pipe(self):
        async def map_f(x):
            await asyncio.sleep(x / 100.0)
            return x * x

        q = push_aiter()
        aiter = map_aiter(map_f, q)
        for _ in range(4):
            q.push(_)
        for _ in range(3, 9):
            q.push(_)
        r = run(get_n(aiter, 10))
        q.stop()
        r.extend(run(get_n(aiter)))
        r1 = sorted([_*_ for _ in range(4)] + [_ * _ for _ in range(3, 9)])
        self.assertEqual(r, r1)

    def test_make_simple_pipeline(self):
        q = push_aiter()
        aiter = flatten_aiter(flatten_aiter(q))
        q.push([
            (0, 0, 1, 0),
            (1, 1, 1, 1),
            (2, 0, 0, 1),
            (3, 1, 2, 0),
            (0, 0, 0, 7),
        ])
        r = run(get_n(aiter, 11))
        self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0])
        r.extend(run(get_n(aiter, 8)))
        q.stop()
        r.extend(run(get_n(aiter)))
        self.assertEqual(r, [0, 0, 1, 0, 1, 1, 1, 1, 2, 0, 0, 1, 3, 1, 2, 0, 0, 0, 0, 7])

    def test_join_aiters(self):
        int_vals = [1, 2, 3, 4]
        str_vals = "abcdefg"

        list_of_lists = [int_vals, str_vals]
        iter_of_aiters = [iter_to_aiter(_) for _ in list_of_lists]
        aiter_of_aiters = iter_to_aiter(iter_of_aiters)
        r = run(get_n(join_aiters(aiter_of_aiters)))

        r1 = [_ for _ in r if isinstance(_, int)]
        r2 = [_ for _ in r if isinstance(_, str)]
        self.assertEqual(r1, int_vals)
        self.assertEqual(r2, list(str_vals))

    def test_join_aiters_1(self):
        # make sure nothing's dropped
        # even if lots of events come in at once
        main_aiter = push_aiter()
        child_aiters = []
        aiter = join_aiters(main_aiter)

        child_aiters.append(push_aiter())
        child_aiters[0].push(100)
        main_aiter.push(child_aiters[0])

        t = run(get_n(aiter, 1))
        self.assertEqual(t, [100])

        child_aiters.append(push_aiter())
        child_aiters[0].push(101)
        child_aiters[1].push(200)
        child_aiters[1].push(201)
        main_aiter.push(child_aiters[1])

        t = run(get_n(aiter, 3))
        self.assertEqual(set(t), set([101, 200, 201]))

        for _ in range(3):
            child_aiters.append(push_aiter())
            main_aiter.push(child_aiters[-1])
        for _, ca in enumerate(child_aiters):
            ca.push((_+1) * 100)
            ca.push((_+1) * 100 + 1)

        t = run(get_n(aiter, len(child_aiters) * 2))
        self.assertEqual(set(t), set([100, 101, 200, 201, 300, 301, 400, 401, 500, 501]))

        child_aiters[-1].push(5000)
        main_aiter.stop()
        t = run(get_n(aiter, 1))
        self.assertEqual(t, [5000])

        for ca in child_aiters:
            ca.push(99)
            ca.stop()
        t = run(get_n(aiter))
        self.assertEqual(t, [99] * len(child_aiters))

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

        q = push_aiter()
        aiter = parallel_map_aiter(make_wait_index(0), 10,
                    parallel_map_aiter(make_wait_index(1), 10,
                        parallel_map_aiter(make_wait_index(2), 10,
                            parallel_map_aiter(make_wait_index(3), 10, q
        ))))
        q.push(*TEST_CASE)
        q.stop()
        r = run(get_n(aiter))
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

        q = push_aiter()
        aiter = flatten_aiter(map_aiter(filter, q))
        q.push(TEST_CASE)
        q.stop()
        r = run(get_n(aiter, 12))
        r1 = [7, 5, 1, 1, 1, 1, 1, 2, 1, 3, 1, 2]
        self.assertEqual(r, r1)

    def test_aiter_forker(self):

        q = push_aiter()
        forker = aiter_forker(q)
        q.push(1, 2, 3, 4, 5)
        r0 = run(get_n(forker, 3))
        f2 = forker.fork()
        q.push(*range(7, 14))
        q.stop()
        r1 = run(get_n(forker))
        r2 = run(get_n(f2))

        self.assertEqual(r0, [1, 2, 3])
        self.assertEqual(r1, [4, 5, 7, 8, 9, 10, 11, 12, 13])
        self.assertEqual(r2, [4, 5, 7, 8, 9, 10, 11, 12, 13])

    def test_aiter_forker_multiple_active(self):
        """
        Multiple forks of an aiter_forker both asking for empty q information
        at the same time. Make sure the second one doesn't block.
        """

        q = push_aiter()
        forker = aiter_forker(q)
        fork_1 = forker.fork(is_active=True)
        fork_2 = forker.fork(is_active=True)
        f1 = asyncio.ensure_future(get_n(fork_1, 1))
        f2 = asyncio.ensure_future(get_n(fork_2, 1))
        run(asyncio.wait([f1, f2], timeout=0.1))
        self.assertFalse(f1.done())
        self.assertFalse(f2.done())
        q.push(1)
        run(asyncio.wait([f1, f2], timeout=0.1))
        self.assertTrue(f1.done())
        self.assertTrue(f2.done())
        r1 = run(f1)
        r2 = run(f2)
        self.assertEqual(r1, [1])
        self.assertEqual(r2, [1])

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

    def test_gated_aiter(self):
        ai = iter_to_aiter(range(3000000000))
        aiter = gated_aiter(ai)
        aiter.push(9)
        r = run(get_n(aiter, 3))
        r.extend(run(get_n(aiter, 4)))
        aiter.push(11)
        aiter.stop()
        r.extend(run(get_n(aiter)))
        self.assertEqual(r, list(range(20)))

    def test_preload_aiter(self):
        q = push_aiter()
        q.push(*list(range(1000)))
        q.stop()

        self.assertEqual(len(q), 1000)
        aiter = preload_aiter(50, q)

        self.assertEqual(len(q), 1000)

        r = run(get_n(aiter, 1))
        self.assertEqual(len(q), 949)
        self.assertEqual(r, [0])

        r = run(get_n(aiter, 10))
        self.assertEqual(r, list(range(1, 11)))
        self.assertEqual(len(q), 939)

        r = run(get_n(aiter))
        self.assertEqual(r, list(range(11, 1000)))
        self.assertEqual(len(q), 0)
