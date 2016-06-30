import asyncio
import unittest

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.networks import MAINNET


def make_getaddrinfo():
    counter = 0

    @asyncio.coroutine
    def fake_getaddrinfo(*args):
        nonlocal counter
        r = []
        for _ in range(10):
            item = ["family", "type", "proto", "canonname", (counter, 8333)]
            r.append(item)
            counter += 1
        return r
    return fake_getaddrinfo


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


class DNSBootstrapTest(unittest.TestCase):
    def test1(self):
        q = dns_bootstrap_host_port_q(MAINNET, getaddrinfo=make_getaddrinfo())
        counter = 0
        while 1:
            r = run(q.get())
            if r is None:
                break
            self.assertEqual(r, (counter, 8333))
            counter += 1
        assert counter == 40
