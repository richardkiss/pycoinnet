import asyncio
import unittest

from pycoinnet.MappingQueue import TaskGroup


class TaskGroupTest(unittest.TestCase):

    def test_TaskGroup(self):
        loop = asyncio.get_event_loop()

        def make_f(results, done_count, done_event, delay_count):
            counter = -1

            async def repeated_f(task_group):
                nonlocal counter
                counter += 1
                if counter >= done_count:
                    done_event.set()
                if counter >= delay_count:
                    await asyncio.sleep(0.05)
                results.append(counter * counter)

            return repeated_f

        d1 = []
        async def is_done():
            d1.append(1)

        async def go(done_count, delay_count, immediate):
            d1[:] = []
            results = []
            done_event = asyncio.Event()
            repeated_f = make_f(results, done_count, done_event, delay_count)
            task_group = TaskGroup(repeated_f=repeated_f, done_callback=is_done, worker_count=1)
            await done_event.wait()
            task_group.cancel(immediate=immediate)
            await task_group.join()
            return results

        r = loop.run_until_complete(go(5, 10, False))
        self.assertEqual(r, [_ * _ for _ in range(10)])
        self.assertEqual(d1, [1])

        r = loop.run_until_complete(go(5, 10, True))
        self.assertEqual(r, [_ * _ for _ in range(6)])
        self.assertEqual(d1, [1])
