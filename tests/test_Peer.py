import asyncio
import unittest

from pycoin.message.InvItem import InvItem

from pycoinnet.Peer import Peer
from pycoinnet.networks import MAINNET
from pycoinnet.version import version_data_for_peer

from .peer_helper import create_peer_pair
from .pipes import create_direct_streams_pair
from .timeless_eventloop import TimelessEventLoop


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


class PeerTest(unittest.TestCase):
    def setUp(self):
        asyncio.set_event_loop(TimelessEventLoop())

    def tearDown(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

    def test_Peer_blank_messages(self):
        p1, p2 = create_peer_pair(MAINNET)
        for t in ["verack", "getaddr", "mempool", "filterclear"]:
            t1 = p1.send_msg(t)
            t2 = run(p2.next_message(unpack_to_dict=False))
            assert t1 is None
            assert t2 == (t, b'')

    @unittest.skip
    def test_Peer_version(self):
        p1, p2 = create_peer_pair(MAINNET)
        version_msg = version_data_for_peer(p1)
        version_msg["relay"] = True
        t1 = p1.send_msg("version", **version_msg)
        assert t1 is None
        t2 = run(p2.next_message())
        assert t2 == ("version", version_msg)

    def test_Peer_inv_getdata_notfound(self):
        p1, p2 = create_peer_pair(MAINNET)
        ii_list = tuple(InvItem(i, bytes([i] * 32)) for i in (1, 2, 3))
        for t in "inv getdata notfound".split():
            d = dict(items=ii_list)
            t1 = p1.send_msg(t, **d)
            assert t1 is None
            t2 = run(p2.next_message())
            assert t2 == (t, d)

    def test_Peer_ping_pong(self):
        p1, p2 = create_peer_pair(MAINNET)
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

    @unittest.skip
    def test_disconnect(self):
        p1, p2 = create_peer_pair(MAINNET)
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

    @unittest.skip
    def test_Peer_multiplex(self):
        p1, p2 = create_peer_pair(MAINNET)

        def make_sync_msg_handler(peer):

            future = asyncio.get_event_loop().create_future()
            handler_id = None

            def handle_msg(name, data):
                future.set_result((name, data))
                peer.remove_msg_handler(handler_id)

            handler_id = peer.add_msg_handler(handle_msg)
            return future

        def make_async_msg_handler(peer):

            future = asyncio.get_event_loop().create_future()
            handler_id = None

            async def handle_msg(name, data):
                await asyncio.sleep(1)
                future.set_result((name, data))
                peer.remove_msg_handler(handler_id)

            handler_id = peer.add_msg_handler(handle_msg)
            return future

        futures = []
        for _ in range(10):
            futures.append(make_sync_msg_handler(p2))
        for _ in range(10):
            futures.append(make_async_msg_handler(p2))
        p1.start_dispatcher()
        p2.start_dispatcher()
        p1.send_msg("verack")
        items = list(asyncio.get_event_loop().run_until_complete(asyncio.wait(futures))[0])
        items = [item.result() for item in items]
        for item in items:
            self.assertEqual(item, ("verack", {}))
        p2.close()
        p1.close()
        asyncio.get_event_loop().run_until_complete(p1.wait_for_cleanup())
        asyncio.get_event_loop().run_until_complete(p2.wait_for_cleanup())


def SKIP_test_BitcoinPeerProtocol():
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


def SKIP_test_eof():
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
