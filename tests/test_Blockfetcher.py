import asyncio
import logging
import unittest

from pycoin.message.InvItem import ITEM_TYPE_BLOCK
from pycoin.networks.registry import network_for_netcode

from tests.helper import make_blocks

from pycoinnet.networks import MAINNET

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.Peer import Peer

from tests.pipes import create_direct_streams_pair
from tests.timeless_eventloop import TimelessEventLoop


def create_pair(loop):
    (r1, w1), (r2, w2) = create_direct_streams_pair(loop=loop)
    p1 = Peer(r1, w1, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    p2 = Peer(r2, w2, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
    return p1, p2


class BlockfetcherTest(unittest.TestCase):
    def setUp(self):
        self.network = network_for_netcode("BTC")
        self.BLOCK_LIST = make_blocks(self.network, 128)
        self.block_lookup = {b.hash(): b for b in self.BLOCK_LIST}
        self.old_loop = asyncio.get_event_loop()
        self.loop = TimelessEventLoop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _test_with_delays(self, d13, d23):
        peer1_3, peer3_1 = create_pair(self.loop)
        peer2_3, peer3_2 = create_pair(self.loop)

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
            block = self.loop.run_until_complete(bf)
            assert block.id() == block1.id()

        for p in peer1_3, peer2_3, peer3_1, peer3_2:
            p.close()
        blockfetcher.close()
        self.loop.stop()
        self.loop.run_forever()

    def test_BlockHandler(self):
        for d13 in (0, 0.5, 5.0):
            for d23 in (0, 2.0, 10.0):
                self._test_with_delays(d13, d23)

    def test_stupid_peer(self):
        # in this test, peer1 only responds to odd numbered blocks
        peer1_3, peer3_1 = create_pair(self.loop)
        peer2_3, peer3_2 = create_pair(self.loop)

        blockfetcher = Blockfetcher()
        blockfetcher.add_peer(peer3_1)
        blockfetcher.add_peer(peer3_2)

        def make_block_msg_handler(peer, delay, block_filter_f=lambda b: True):

            @asyncio.coroutine
            def msg_handler(msg, data):
                if msg == 'getdata':
                    for inv in data.get("items"):
                        if inv.item_type == ITEM_TYPE_BLOCK:
                            yield from asyncio.sleep(delay)
                            b = self.block_lookup[inv.data]
                            if block_filter_f(b):
                                try:
                                    peer.send_msg("block", block=b)
                                except Exception as ex:
                                    print(ex)
            return msg_handler

        peer1_3.add_msg_handler(make_block_msg_handler(
            peer1_3, delay=0.5, block_filter_f=lambda block: block.hash()[-1] & 0x1 == 0))
        peer2_3.add_msg_handler(make_block_msg_handler(peer2_3, delay=0.5))

        peer3_1.add_msg_handler(blockfetcher.handle_msg)
        peer3_2.add_msg_handler(blockfetcher.handle_msg)

        for p in [peer1_3, peer2_3, peer3_1, peer3_2]:
            p.start_dispatcher()

        block_hash_priority_pair_list = [(b.hash(), idx) for idx, b in enumerate(self.BLOCK_LIST)]
        block_futures = blockfetcher.fetch_blocks(block_hash_priority_pair_list)

        for bf, block1 in zip(block_futures, self.BLOCK_LIST):
            block = self.loop.run_until_complete(bf)
            assert block.id() == block1.id()

        for p in peer1_3, peer2_3, peer3_1, peer3_2:
            p.close()
        blockfetcher.close()
        self.loop.stop()
        self.loop.run_forever()


asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
