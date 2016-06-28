import asyncio
import unittest

from tests.helper import make_blocks

from pycoinnet.networks import MAINNET
from pycoinnet.msg.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.PeerProtocol import PeerProtocol

from tests.timeless_eventloop import create_timeless_transport_pair, use_timeless_eventloop


def create_pair():
    (t1, p1), (t2, p2) = create_timeless_transport_pair(lambda: PeerProtocol(MAINNET))
    return p1, p2


@asyncio.coroutine
def handle_getdata(peer, block_lookup, delay=0.1):
    while True:
        try:
            msg, data = yield from peer.next_message()
        except Exception:
            break
        if msg == 'getdata':
            for inv in data.get("items"):
                if inv.item_type == ITEM_TYPE_BLOCK:
                    yield from asyncio.sleep(delay)
                    b = block_lookup[inv.data]
                    peer.send_msg("block", block=b)


@asyncio.coroutine
def handle_getdata3(peer, bf):
    while True:
        try:
            msg, data = yield from peer.next_message()
        except Exception:
            break
        bf.handle_msg(msg, data)


class BlockfetcherTest(unittest.TestCase):
    def setUp(self):
        use_timeless_eventloop()
        self.BLOCK_LIST = make_blocks(128)
        self.block_lookup = {b.hash(): b for b in self.BLOCK_LIST}

    def _test_with_delays(self, d13, d23):
        loop = asyncio.get_event_loop()
        peer1_3, peer3_1 = create_pair()
        peer2_3, peer3_2 = create_pair()

        blockfetcher = Blockfetcher()
        blockfetcher.add_peer(peer3_1)
        blockfetcher.add_peer(peer3_2)

        loop.create_task(handle_getdata(peer1_3, self.block_lookup, delay=d13))
        loop.create_task(handle_getdata(peer2_3, self.block_lookup, delay=d23))

        loop.create_task(handle_getdata3(peer3_1, blockfetcher))
        loop.create_task(handle_getdata3(peer3_2, blockfetcher))

        block_hash_priority_pair_list = [(b.hash(), idx) for idx, b in enumerate(self.BLOCK_LIST)]
        block_futures = blockfetcher.fetch_blocks(block_hash_priority_pair_list)

        for bf, block1 in zip(block_futures, self.BLOCK_LIST):
            block = loop.run_until_complete(bf)
            assert block.id() == block1.id()

        for p in peer1_3, peer2_3, peer3_1, peer3_2:
            p._transport.close()
        loop.stop()
        loop.run_forever()

    def test_BlockHandler(self):
        for d13 in (0, 0.5, 5.0):
            for d23 in (0, 2.0, 10.0):
                self._test_with_delays(d13, d23)
