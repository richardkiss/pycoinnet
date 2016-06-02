import asyncio
import unittest

from pycoinnet.PeerProtocol import PeerProtocol
from pycoinnet.msg.InvItem import InvItem
from pycoinnet.msg.PeerAddress import PeerAddress
from pycoinnet.networks import MAINNET

from tests.pipes import create_pipe_pairs


# from peer.helper import (PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN,
# VERSION_MSG, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN)


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


def connect_pair():
    """Return a pair PeerProtocol objects connected through a pipe"""
    return run(create_pipe_pairs(lambda: PeerProtocol(MAINNET)))


class PeerProtocolTest(unittest.TestCase):

    def test_PeerProtocol_blank_messages(self):
        pp1, pp2 = connect_pair()
        for t in ["verack", "getaddr", "mempool", "filterclear"]:
            t1 = pp1.send_msg(t)
            t2 = run(pp2.next_message(unpack_to_dict=False))
            assert t1 is None
            assert t2 == (t, b'')

    def test_PeerProtocol_version(self):
        pp1, pp2 = connect_pair()
        VERSION_MSG = dict(
                version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
                remote_address=PeerAddress(1, "127.0.0.2", 6111),
                local_address=PeerAddress(1, "127.0.0.1", 6111),
                nonce=3412075413544046060,
                last_block_index=0
            )
        import pdb; pdb.set_trace()
        t1 = pp1.send_msg("version", **VERSION_MSG)
        assert t1 is None
        t2 = run(pp2.next_message())
        assert t2 == ("version", VERSION_MSG)

    def test_PeerProtocol_inv_getdata_notfound(self):
        pp1, pp2 = connect_pair()
        ii_list = tuple(InvItem(i, bytes([i] * 32)) for i in (1, 2, 3))
        for t in "inv getdata notfound".split():
            d = dict(items=ii_list)
            t1 = pp1.send_msg(t, **d)
            assert t1 is None
            t2 = run(pp2.next_message())
            assert t2 == (t, d)

    def test_PeerProtocol_ping_pong(self):
        pp1, pp2 = connect_pair()
        for nonce in (1, 11, 111123, 8888888817129829):
            for t in "ping pong".split():
                d = dict(nonce=nonce)
                t1 = pp1.send_msg(t, **d)
                assert t1 is None
                t1 = pp2.send_msg(t, **d)
                assert t1 is None
                t2 = run(pp2.next_message())
                assert t2 == (t, d)
                t2 = run(pp1.next_message())
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

    def ztest_raw_data(self):
        DATA = []

        def write_f(data):
            DATA.append(data)

        peer = PeerProtocol(MAGIC_HEADER)
        pt = PeerTransport(write_f)
        peer.connection_made(pt)
        assert DATA == []
        
        peer.send_msg("version", **VERSION_MSG)
        assert DATA[-1] == VERSION_MSG_BIN
        
        peer.send_msg("verack")
        assert len(DATA) == 2
        assert DATA[-1] == VERACK_MSG_BIN

        peer.send_msg("mempool")
        assert len(DATA) == 3
        assert DATA[-1] == b'foodmempool\x00\x00\x00\x00\x00\x00\x00\x00\x00]\xf6\xe0\xe2'


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
