import asyncio
import unittest

from pycoin.message.InvItem import InvItem
from pycoin.message.PeerAddress import PeerAddress

from pycoinnet.Peer import Peer
from pycoinnet.networks import MAINNET

from tests.pipes import create_direct_streams_pair
from tests.timeless_eventloop import TimelessEventLoop


# from peer.helper import (PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN,
# VERSION_MSG, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN)


def ip_2_bin(ip):
    return bytes(int(x) for x in ip.split("."))


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


class PeerTest(unittest.TestCase):
    def setUp(self):
        asyncio.set_event_loop(TimelessEventLoop())

    def tearDown(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

    def create_peer_pair(self):
        (r1, w1), (r2, w2) = create_direct_streams_pair()
        p1 = Peer(r1, w1, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        p2 = Peer(r2, w2, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        return p1, p2

    def test_Peer_blank_messages(self):
        p1, p2 = self.create_peer_pair()
        for t in ["verack", "getaddr", "mempool", "filterclear"]:
            t1 = p1.send_msg(t)
            t2 = run(p2.next_message(unpack_to_dict=False))
            assert t1 is None
            assert t2 == (t, b'')

    def test_Peer_version(self):
        p1, p2 = self.create_peer_pair()
        VERSION_MSG = dict(
                version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
                remote_address=PeerAddress(1, ip_2_bin("127.0.0.2"), 6111),
                local_address=PeerAddress(1, ip_2_bin("127.0.0.1"), 6111),
                nonce=3412075413544046060,
                last_block_index=0
            )
        t1 = p1.send_msg("version", **VERSION_MSG)
        assert t1 is None
        t2 = run(p2.next_message())
        assert t2 == ("version", VERSION_MSG)

    def test_Peer_inv_getdata_notfound(self):
        p1, p2 = self.create_peer_pair()
        ii_list = tuple(InvItem(i, bytes([i] * 32)) for i in (1, 2, 3))
        for t in "inv getdata notfound".split():
            d = dict(items=ii_list)
            t1 = p1.send_msg(t, **d)
            assert t1 is None
            t2 = run(p2.next_message())
            assert t2 == (t, d)

    def test_Peer_ping_pong(self):
        p1, p2 = self.create_peer_pair()
        for nonce in (1, 11, 111123, 8888888817129829):
            for t in "ping pong".split():
                d = dict(nonce=nonce)
                t1 = p1.send_msg(t, **d)
                assert t1 is None
                t1 = p2.send_msg(t, **d)
                assert t1 is None
                t2 = run(p2.next_message())
                assert t2 == (t, d)
                t2 = run(p1.next_message())
                assert t2 == (t, d)

    # TODO:
    # add tests for the following messages:
    """
    'addr': "date_address_tuples:[LA]",
    'getblocks': "version:L hashes:[#] hash_stop:#",
    'getheaders': "version:L hashes:[#] hash_stop:#",
    'tx': "tx:T",
    'block': "block:B",
    'headers': "headers:[zI]",
    'filterload': "filter:[1] hash_function_count:L tweak:L flags:b",
    'filteradd': "data:[1]",
    'merkleblock': (
    "header:z total_transactions:L hashes:[#] flags:[1]"
    ),
    'alert': "payload:S signature:S",
    """

    def test_disconnect(self):
        p1, p2 = self.create_peer_pair()
        for nonce in (1, 11, 111123, 8888888817129829):
            for t in "ping pong".split():
                d = dict(nonce=nonce)
                t1 = p1.send_msg(t, **d)
                assert t1 is None
                t1 = p2.send_msg(t, **d)
                assert t1 is None
                t2 = run(p2.next_message())
                assert t2 == (t, d)
                t2 = run(p1.next_message())
                assert t2 == (t, d)
        p2.send_msg("ping", nonce=100)
        t1 = run(p1.next_message())
        assert t1 == ("ping", dict(nonce=100))
        p2.send_msg("ping", nonce=100)
        p2.close()
        run(p1.next_message())
        with self.assertRaises(EOFError):
            run(p1.next_message())


def ztest_BitcoinPeerProtocol_read():
    DATA = []

    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(write_f)
    peer.connection_made(pt)

    next_message = peer.new_get_next_message_f()

    @asyncio.coroutine
    def async_test():
        t = []
        name, data = yield from next_message()
        t.append((name, data))
        name, data = yield from next_message()
        t.append((name, data))
        return t

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    t = asyncio.get_event_loop().run_until_complete(async_test())
    assert len(t) == 2
    assert t[0] == ('version', VERSION_MSG)
    assert t[1] == ('verack', {})


def ztest_BitcoinPeerProtocol_multiplex():
    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(None)
    peer.connection_made(pt)

    next_message_list = [peer.new_get_next_message_f() for i in range(50)]

    COUNT = 0

    @asyncio.coroutine
    def async_test(next_message):
        name, data = yield from next_message()
        assert name == 'version'
        assert data == VERSION_MSG
        name, data = yield from next_message()
        assert name == 'verack'
        assert data == {}
        nonlocal COUNT
        COUNT += 1

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    asyncio.get_event_loop().run_until_complete(
        asyncio.wait([asyncio.Task(async_test(nm)) for nm in next_message_list]))
    assert COUNT == 50


def ztest_BitcoinPeerProtocol():
    @asyncio.coroutine
    def do_test(peer, vm1, vm2):
        next_message = peer.new_get_next_message_f()

        peer.send_msg("version", **vm1)
        message_name, data = yield from next_message()
        assert message_name == 'version'
        assert data == vm2

        peer.send_msg("verack")
        message_name, data = yield from next_message()
        assert message_name == 'verack'
        assert data == {}

        peer.send_msg("getaddr")
        message_name, data = yield from next_message()
        assert message_name == 'getaddr'
        assert data == {}
        return True

    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received)
    pt2 = PeerTransport(peer1.data_received)

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)

    f1 = asyncio.Task(do_test(peer1, VERSION_MSG, VERSION_MSG_2))
    f2 = asyncio.Task(do_test(peer2, VERSION_MSG_2, VERSION_MSG))

    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))
    for f in done:
        assert f.result() is True


def ztest_eof():
    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(None)
    peer.connection_made(pt)

    COUNT = 0

    @asyncio.coroutine
    def async_listen(next_message):
        count = 0
        try:
            while True:
                name, data = yield from next_message()
                count += 1
        except EOFError:
            pass
        assert count == 2
        nonlocal COUNT
        COUNT += 1

    tasks = [asyncio.Task(async_listen(peer.new_get_next_message_f())) for i in range(50)]

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    # end of stream
    peer.connection_lost(None)

    # give everyone a chance to run (but no one finishes)
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks, timeout=5))

    assert COUNT == 50
