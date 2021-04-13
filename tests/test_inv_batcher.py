import asyncio
import unittest

from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX
from pycoin.networks.registry import network_for_netcode

from .helper import make_tx
from .peer_helper import create_handshaked_peer_pair

from pycoinnet.inv_batcher import InvBatcher


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


class InvBatcherTest(unittest.TestCase):
    def setUp(self):
        self.network = network_for_netcode("BTC")

    def test_fetch_tx(self):

        txs = [make_tx(self.network, _) for _ in range(24)]
        pp1, pp2 = create_handshaked_peer_pair(self.network)

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
        txs = [make_tx(self.network, _) for _ in range(24)]
        pp1, pp2 = create_handshaked_peer_pair(self.network)

        inv_batcher = InvBatcher()
        run(inv_batcher.add_peer(pp1))
        inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in txs]
        pp1.set_request_callback("tx", inv_batcher.handle_tx_event)

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
        m = run(pp1.next_message())
        self.assertIsNone(m)
