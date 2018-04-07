import asyncio
import unittest

from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX
from pycoin.networks.registry import network_for_netcode

from .helper import make_tx

from pycoinnet.Peer import Peer
from pycoinnet.PeerEvent import PeerEvent
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.networks import MAINNET

from tests.pipes import create_pipe_streams_pair


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


def create_peer_pair():
    (r1, w1), (r2, w2) = run(create_pipe_streams_pair())
    p1 = Peer(r1, w1, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    p2 = Peer(r2, w2, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    return p1, p2


class InvBatcherTest(unittest.TestCase):
    def setUp(self):
        self.network = network_for_netcode("BTC")

    def test_fetch_tx(self):

        txs = [make_tx(_) for _ in range(24)]
        pp1, pp2 = create_peer_pair()

        pp1 = PeerEvent(pp1)

        async def go():
            inv_batcher = InvBatcher()
            await inv_batcher.add_peer(pp1)
            inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
            pp1.set_request_callback("tx", inv_batcher.handle_tx_event)

            futures = [await inv_batcher.inv_item_to_future(_) for _ in inv_items]

            for tx in txs:
                pp2.send_msg("tx", tx=tx)

            the_txs = [await f for f in futures]
            for t1, t2 in zip(txs, the_txs):
                self.assertEqual(t1.id(), t2.id())

        run(go())

    def test_close(self):
        """
        Close after 10 transactions.
        """
        txs = [make_tx(_) for _ in range(24)]
        pp1, pp2 = create_peer_pair()
        pp1_e = PeerEvent(pp1)

        inv_batcher = InvBatcher()
        run(inv_batcher.add_peer(pp1_e))
        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        pp1_e.set_request_callback("tx", inv_batcher.handle_tx_event)

        async def go():
            futures = [(await inv_batcher.inv_item_to_future(_)) for _ in inv_items]

            inv_msg, data = await pp2.next_message()
            for tx in txs[:10]:
                pp2.send_msg("tx", tx=tx)
            pp2.write_eof()
            the_txs = [await _ for _ in futures[:10]]
            return the_txs

        the_txs = run(go())
        self.assertEqual(len(the_txs), 10)
        for t1, t2 in zip(txs, the_txs):
            self.assertEqual(t1.id(), t2.id())
        with self.assertRaises(EOFError):
            run(pp1.next_message())
