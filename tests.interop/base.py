
import os
import unittest


class InteropTest(unittest.UnitTest):
    def setUp(self):
        try:
            host_port = os.getenv("BITCOIND_HOSTPORT")
            self.host, self.port = host_port.split(":")
            self.port = int(self.port)
        except Exception:
            raise ValueError('need to set BITCOIND_HOSTPORT="127.0.0.1:8333" for example')
