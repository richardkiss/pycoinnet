import asyncio
import time
import unittest

from tests.timeless_eventloop import use_timeless_eventloop


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


class TheTest(unittest.TestCase):
    def setUp(self):
        use_timeless_eventloop()

    def test_loop(self):
        loop = asyncio.get_event_loop()
        a1 = time.time()
        b1 = loop.time()
        run(asyncio.sleep(50))
        a2 = time.time()
        b2 = loop.time()
        assert a2 - a1 < 0.1
        assert b2 - b1 > 50.0
