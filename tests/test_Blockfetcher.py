import asyncio
import unittest

from tests.helper import make_blocks

from pycoinnet.networks import MAINNET
from pycoinnet.msg.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.Peer import Peer

from tests.timeless_eventloop import create_timeless_streams_pair, use_timeless_eventloop


run = asyncio.get_event_loop().run_until_complete


def create_pair():
    (r1, w1), (r2, w2) = create_timeless_streams_pair()
    p1 = Peer(r1, w1, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    p2 = Peer(r2, w2, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    return p1, p2


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

        def make_block_msg_handler(peer, delay):

            @asyncio.coroutine
            def msg_handler(msg, data):
                if msg == 'getdata':
                    for inv in data.get("items"):
                        if inv.item_type == ITEM_TYPE_BLOCK:
                            yield from asyncio.sleep(delay)
                            b = self.block_lookup[inv.data]
                            try:
                                peer.send_msg("block", block=b)
                            except Exception as ex:
                                print(ex)
                                import pdb
                                pdb.set_trace()
                                print("foo")
            return msg_handler

        peer1_3.add_msg_handler(make_block_msg_handler(peer1_3, delay=d13))
        peer2_3.add_msg_handler(make_block_msg_handler(peer2_3, delay=d23))

        peer3_1.add_msg_handler(blockfetcher.handle_msg)
        peer3_2.add_msg_handler(blockfetcher.handle_msg)

        for p in [peer1_3, peer2_3, peer3_1, peer3_2]:
            p.start_dispatcher()

        block_hash_priority_pair_list = [(b.hash(), idx) for idx, b in enumerate(self.BLOCK_LIST)]
        block_futures = blockfetcher.fetch_blocks(block_hash_priority_pair_list)

        for bf, block1 in zip(block_futures, self.BLOCK_LIST):
            block = loop.run_until_complete(bf)
            assert block.id() == block1.id()

        for p in peer1_3, peer2_3, peer3_1, peer3_2:
            p.close()
        blockfetcher.close()
        loop.stop()
        loop.run_forever()

    def test_BlockHandler(self):
        for d13 in (0, 0.5, 5.0):
            for d23 in (0, 2.0, 10.0):
                self._test_with_delays(d13, d23)
