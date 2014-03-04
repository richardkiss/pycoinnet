from pycoinnet.util.debug_help import asyncio

from pycoinnet.peer.tests.helper import create_handshaked_peers, make_tx
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK

def test_InvCollector_simple():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()

    # the peer1a, 1b, 1c represent the local peer

    TX_LIST = [make_tx(i) for i in range(10)]

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        inv_item_q = inv_collector.new_inv_item_queue()
        while len(r) < 10:
            inv_item = yield from inv_item_q.get()
            v = yield from inv_collector.fetch(inv_item)
            r.append(v)
        return r

    @asyncio.coroutine
    def run_remote_peer(peer, txs):
        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        while True:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
        return r

    f1 = asyncio.Task(run_local_peer([peer1_2]))
    f2 = asyncio.Task(run_remote_peer(peer2, TX_LIST))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1]))
    r = done.pop().result()
    assert len(r) == 10
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST]


def test_InvCollector():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()
    peer1_3, peer3 = create_handshaked_peers()
    peer1_4, peer4 = create_handshaked_peers()

    # the peer1a, 1b, 1c represent the local peer

    TX_LIST = [make_tx(i) for i in range(100)]

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        inv_item_q = inv_collector.new_inv_item_queue()
        while len(r) < 90:
            inv_item = yield from inv_item_q.get()
            v = yield from inv_collector.fetch(inv_item)
            r.append(v)
        return r

    @asyncio.coroutine
    def run_remote_peer(peer, txs):
        tx_db = dict((tx.hash(), tx) for tx in txs)
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()
        while True:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                for inv_item in t[1]["items"]:
                    peer.send_msg("tx", tx=tx_db[inv_item.data])
        return r

    futures = []
    for peer, txs in [(peer2, TX_LIST[:30]), (peer3, TX_LIST[30:60]), (peer4, TX_LIST[60:90])]:
        f = asyncio.Task(run_remote_peer(peer, txs))
        futures.append(f)

    f = asyncio.Task(run_local_peer([peer1_2, peer1_3, peer1_4]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f]))
    r = done.pop().result()
    assert len(r) == 90
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST[:90]]


def test_TxCollector_notfound():
    peer1_2, peer2 = create_handshaked_peers()

    TX_LIST = [make_tx(i) for i in range(10)]

    @asyncio.coroutine
    def run_peer_2(peer, txs):
        # this peer will immediately advertise five transactions
        # But when they are requested, it will say they are "notfound".
        # Then it will sleep 0.25 s, then advertise one transaction,
        # then send it when requested.
        next_message = peer.new_get_next_message_f()

        tx_db = dict((tx.hash(), tx) for tx in txs[5:])
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        while 1:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                found = []
                not_found = []
                for inv_item in t[1]["items"]:
                    if inv_item.data in tx_db:
                        found.append(tx_db[inv_item.data])
                    else:
                        not_found.append(inv_item)
                if not_found:
                    peer.send_msg("notfound", items=not_found)
                for tx in found:
                    peer.send_msg("tx", tx=tx)

        return r

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        inv_item_q = inv_collector.new_inv_item_queue()
        while len(r) < 5:
            yield from asyncio.sleep(0.1)
            inv_item = yield from inv_item_q.get()
            try:
                v = yield from asyncio.wait_for(inv_collector.fetch(inv_item), timeout=0.5)
                if v:
                    r.append(v)
            except asyncio.TimeoutError:
                pass
        return r

    f2 = asyncio.Task(run_peer_2(peer2, TX_LIST))

    f = asyncio.Task(run_local_peer([peer1_2]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f], timeout=7.5))
    r = done.pop().result()
    assert len(r) == 5
    assert [tx.hash() for tx in r] == [tx.hash() for tx in TX_LIST[5:]]


def test_TxCollector_retry():
    # create some peers
    peer1_2, peer2 = create_handshaked_peers()
    peer1_3, peer3 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.3")

    TX_LIST = [make_tx(i) for i in range(10)]
    TX_LIST.sort(key=lambda tx: tx.id())

    @asyncio.coroutine
    def run_remote_peer(peer, txs, in_db_count, delay):
        # this peer will immediately advertise the ten transactions
        # But when they are requested, it will only send one,
        # and "notfound" eight.
        yield from asyncio.sleep(delay)
        tx_db = dict((tx.hash(), tx) for tx in txs[:in_db_count])
        r = []

        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        peer.send_msg("inv", items=inv_items)

        next_message = peer.new_get_next_message_f()

        while True:
            t = yield from next_message()
            r.append(t)
            if t[0] == 'getdata':
                found = []
                not_found = []
                yield from asyncio.sleep(0.1)
                for inv_item in t[1]["items"]:
                    if inv_item.data in tx_db:
                        found.append(tx_db[inv_item.data])
                    else:
                        not_found.append(inv_item)
                if not_found:
                    if len(not_found) == 9:
                        not_found = not_found[:8]
                    peer.send_msg("notfound", items=not_found)
                for tx in found:
                    peer.send_msg("tx", tx=tx)
        return r

    @asyncio.coroutine
    def run_local_peer(peer_list):
        inv_collector = InvCollector()
        for peer in peer_list:
            inv_collector.add_peer(peer)
        r = []
        inv_item_q = inv_collector.new_inv_item_queue()
        
        @asyncio.coroutine
        def _do_fetch(inv_collector, inv_item, r):
            v = yield from inv_collector.fetch(inv_item, peer_timeout=3.0)
            if v:
                r.append(v)

        for i in range(10):
            inv_item = yield from inv_item_q.get()
            asyncio.Task(_do_fetch(inv_collector, inv_item, r))
        while len(r) < 10:
            yield from asyncio.sleep(0.1)
        return r

    f2 = asyncio.Task(run_remote_peer(peer2, TX_LIST, 1, 0.2))
    f3 = asyncio.Task(run_remote_peer(peer3, TX_LIST, 10, 1.0))

    f = asyncio.Task(run_local_peer([peer1_2, peer1_3]))
    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f], timeout=7.5))
    assert len(done) == 1
    r = done.pop().result()
    assert len(r) == 10
    assert set(tx.hash() for tx in r) == set(tx.hash() for tx in TX_LIST)


import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
