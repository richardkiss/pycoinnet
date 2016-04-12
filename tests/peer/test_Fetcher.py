from pycoinnet.util.debug_help import asyncio
import hashlib

from pycoin.tx.Tx import Tx, TxIn, TxOut

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK
from pycoinnet.helpers import standards
from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol, BitcoinProtocolError
from pycoinnet.peer.Fetcher import Fetcher
from pycoinnet.PeerAddress import PeerAddress

from tests.helper import make_hash, make_tx
from tests.peer.helper import PeerTransport, MAGIC_HEADER, VERSION_MSG_BIN, VERSION_MSG, VERSION_MSG, VERSION_MSG_2, VERACK_MSG_BIN, create_peers

def create_handshaked_peers():
    peer1, peer2 = create_peers()
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, VERSION_MSG), initial_handshake(peer2, VERSION_MSG_2)]))
    return peer1, peer2

def mi(the_hash):
    return InvItem(ITEM_TYPE_TX, the_hash)

def test_fetcher():
    peer1, peer2 = create_peers()

    TX_LIST = [make_tx(i) for i in range(100)]

    from pycoinnet.message import pack_from_data
    msg_data = pack_from_data("tx", tx=TX_LIST[0])

    item1 = [InvItem(1, TX_LIST[0].hash())]
    item2 = [InvItem(1, TX_LIST[i].hash()) for i in range(2)]
    msg_data = pack_from_data("getdata", items=item1)
    msg_data = pack_from_data("getdata", items=item2)
    msg_data = pack_from_data("notfound", items=item1)
    msg_data = pack_from_data("notfound", items=item2)

    @asyncio.coroutine
    def run_peer1():
        r = []
        
        yield from standards.initial_handshake(peer1, VERSION_MSG)
        next_message = peer1.new_get_next_message_f()

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("tx", tx=TX_LIST[0])

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("notfound", items=[InvItem(ITEM_TYPE_TX, TX_LIST[1].hash())])

        t = yield from next_message()
        r.append(t)
        peer1.send_msg("tx", tx=TX_LIST[2])

        t = yield from next_message()
        r.append(t)
        items = [InvItem(ITEM_TYPE_TX, TX_LIST[3].hash())]
        items.append([InvItem(ITEM_TYPE_TX, TX_LIST[5].hash())])
        peer1.send_msg("notfound", items=items)
        peer1.send_msg("tx", tx=TX_LIST[4])

        return r

    @asyncio.coroutine
    def run_peer2():
        r = []
        yield from standards.initial_handshake(peer2, VERSION_MSG_2)
        tx_fetcher = Fetcher(peer2)

        tx = yield from tx_fetcher.fetch(mi(TX_LIST[0].hash()), timeout=5)
        r.append(tx)

        tx = yield from tx_fetcher.fetch(mi(TX_LIST[1].hash()))
        r.append(tx)

        tx = yield from tx_fetcher.fetch(mi(TX_LIST[2].hash()))
        r.append(tx)

        f1 = asyncio.Task(tx_fetcher.fetch(mi(TX_LIST[3].hash())))
        f2 = asyncio.Task(tx_fetcher.fetch(mi(TX_LIST[4].hash())))
        f3 = asyncio.Task(tx_fetcher.fetch(mi(TX_LIST[5].hash())))
        yield from asyncio.wait([f1, f2, f3])

        r.append(f1.result())
        r.append(f2.result())
        r.append(f3.result())

        return r


    f1 = asyncio.Task(run_peer1())
    f2 = asyncio.Task(run_peer2())

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))

    r = f1.result()
    assert len(r) == 4
    assert r[0] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[0].hash()),)))
    assert r[1] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[1].hash()),)))
    assert r[2] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[2].hash()),)))
    assert r[3] == ('getdata', dict(items=tuple(InvItem(ITEM_TYPE_TX, TX_LIST[i].hash()) for i in range(3,6))))

    r = f2.result()
    assert len(r) == 6
    assert r[0].hash() == TX_LIST[0].hash()
    assert r[1] == None
    assert r[2].hash() == TX_LIST[2].hash()
    assert r[3] == None
    assert r[4].hash() == TX_LIST[4].hash()
    assert r[5] == None


def test_fetcher_timeout():
    peer1, peer2 = create_peers()

    TX_LIST = [make_tx(i) for i in range(100)]

    @asyncio.coroutine
    def run_peer1():
        r = []
        yield from standards.initial_handshake(peer1, VERSION_MSG)
        next_message = peer1.new_get_next_message_f()
        t = yield from next_message()
        r.append(t)
        return r

    @asyncio.coroutine
    def run_peer2():
        r = []
        yield from standards.initial_handshake(peer2, VERSION_MSG_2)
        tx_fetcher = Fetcher(peer2)
        tx = yield from tx_fetcher.fetch(mi(TX_LIST[0].hash()), timeout=2)
        r.append(tx)
        return r

    f1 = asyncio.Task(run_peer1())
    f2 = asyncio.Task(run_peer2())

    asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2]))

    r = f1.result()
    assert len(r) == 1
    assert r[0] == ('getdata', dict(items=(InvItem(ITEM_TYPE_TX, TX_LIST[0].hash()),)))

    r = f2.result()
    assert len(r) == 1
    assert r[0] == None
