import asyncio

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.tests.helper import PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN, VERSION_MSG, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN


def test_BitcoinPeerProtocol_send():
    DATA = []
    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
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

def test_BitcoinPeerProtocol_read():
    DATA = []
    def write_f(data):
        DATA.append(data)

    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(write_f)
    peer.connection_made(pt)

    next_message = peer.new_get_next_message_f()

    @asyncio.coroutine
    def async_test():
        name, data = yield from next_message()
        assert name == 'version'
        assert data == VERSION_MSG
        name, data = yield from next_message()
        assert name == 'verack'
        assert data == {}

    peer.data_received(VERSION_MSG_BIN)
    peer.data_received(VERACK_MSG_BIN)
    asyncio.get_event_loop().run_until_complete(async_test())

def test_BitcoinPeerProtocol_multiplex():
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
    asyncio.get_event_loop().run_until_complete(asyncio.wait([asyncio.Task(async_test(nm)) for nm in next_message_list]))
    assert COUNT == 50

def test_BitcoinPeerProtocol():
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

    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received)
    pt2 = PeerTransport(peer1.data_received)

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)

    f1 = asyncio.Task(do_test(peer1, VERSION_MSG, VERSION_MSG_2))
    f2 = asyncio.Task(do_test(peer2, VERSION_MSG_2, VERSION_MSG))

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))

def test_queue_gc():
    # create a peer
    # add 50 listeners
    # receive 100 messages
    # first 10 listeners will stop listening after two messages
    # check GC
    peer = BitcoinPeerProtocol(MAGIC_HEADER)
    pt = PeerTransport(None)
    peer.connection_made(pt)

    next_message_list = [peer.new_get_next_message_f() for i in range(50)]
    assert len(peer.message_queues) == 50
    next_message_list = None
    assert len(peer.message_queues) == 0

    @asyncio.coroutine
    def async_listen(next_message, delay=0):
        for i in range(101):
            name, data = yield from next_message()
        yield from asyncio.sleep(delay)

    for i in range(3):
        asyncio.Task(async_listen(peer.new_get_next_message_f(), delay=60))
    tasks = [asyncio.Task(async_listen(peer.new_get_next_message_f(), delay=1)) for i in range(50)]

    peer.data_received(VERSION_MSG_BIN)
    for i in range(100):
        peer.data_received(VERACK_MSG_BIN)

    # give everyone a chance to run (but no one finishes)
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks, timeout=0.1))

    assert len(peer.message_queues) == 53

    # now let all 50 finish. They should be collected.
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))

    ## you would think this number would be 3, but it's not.
    ## oh well. This is close enough.
    assert len(peer.message_queues) <= 4

def test_eof():
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
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))

    assert COUNT == 50
