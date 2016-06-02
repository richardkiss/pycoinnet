
import asyncio
import os
import unittest

from pycoinnet.PeerProtocol import PeerProtocol
from pycoinnet.msg.PeerAddress import PeerAddress
from pycoinnet.networks import MAINNET


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=0
)


class InteropTest(unittest.TestCase):
    def setUp(self):
        try:
            host_port = os.getenv("BITCOIND_HOSTPORT")
            self.host, self.port = host_port.split(":")
            self.port = int(self.port)
        except Exception:
            raise ValueError('need to set BITCOIND_HOSTPORT="127.0.0.1:8333" for example')

    def test_connect(self):
        loop = asyncio.get_event_loop()
        transport, protocol = run(loop.create_connection(
            lambda: PeerProtocol(MAINNET), host=self.host, port=self.port))
        protocol.send_msg("version", **VERSION_MSG)
        msg = run(protocol.next_message())
        assert msg[0] == 'version'
        protocol.send_msg("verack")
        msg = run(protocol.next_message())
        assert msg[0] == 'verack'
        protocol.send_msg("mempool")
        msg_name, msg_data = run(protocol.next_message())
        assert msg_name == 'inv'
        if msg_name == 'inv':
            items = msg_data.get("items")
            protocol.send_msg("getdata", items=items)
            for _ in range(len(items)):
                msg_name, msg_data = run(protocol.next_message())
                print(msg_data.get("tx"))
