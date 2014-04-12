from pycoinnet.util.debug_help import asyncio

from pycoinnet.peer.tests.helper import create_handshaked_peers, make_tx
from pycoinnet.peergroup.InvCollector import InvCollector
from pycoinnet.peergroup.TxHandler import TxHandler

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol


def test_TxHandler_simple():
    # create some peers
    peer1_2, peer2_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.2")
    peer1_3, peer3_1 = create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.3")

    TX_LIST = [make_tx(i) for i in range(20)]

    @asyncio.coroutine
    def run_client(peer_list, tx_list):
        tx_store = {}
        inv_collector = InvCollector()
        tx_handler = TxHandler(inv_collector, tx_store)
        for tx in tx_list:
            tx_handler.add_tx(tx)
        for peer in peer_list:
            inv_collector.add_peer(peer)
            tx_handler.add_peer(peer)
        while len(tx_store) < 20:
            yield from asyncio.sleep(0.1)
        return tx_store

    f1 = asyncio.Task(run_client([peer1_2, peer1_3], []))
    f2 = asyncio.Task(run_client([peer2_1], TX_LIST[:10]))
    f3 = asyncio.Task(run_client([peer3_1], TX_LIST[10:]))

    done, pending = asyncio.get_event_loop().run_until_complete(asyncio.wait([f1, f2, f3], timeout=5.0))
    assert len(done) == 3
    assert len(pending) == 0
    for i in range(3):
        r = done.pop().result()
        assert len(r) == 20
        assert set(tx.hash() for tx in r.values()) == set(tx.hash() for tx in TX_LIST)


import logging
asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
