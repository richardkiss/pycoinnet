import asyncio
import unittest

from pdb import set_trace as debugger

from pycoin.message.InvItem import ITEM_TYPE_BLOCK
from pycoin.networks.registry import network_for_netcode

from .helper import make_blocks

from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.blockcatchup import fetch_blocks_after

from pycoinnet.BlockChainView import BlockChainView

from .peer_helper import create_handshaked_peer_pair
from .timeless_eventloop import TimelessEventLoop

from pycoinnet.cmds.common import init_logging


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


init_logging()


class BlockcatchupTest(unittest.TestCase):
    def setUp(self):
        self.network = network_for_netcode("BTC")
        self.BLOCK_LIST = make_blocks(self.network, 4)
        self.block_lookup = {b.hash(): b for b in self.BLOCK_LIST}
        self.old_loop = asyncio.get_event_loop()
        self.loop = TimelessEventLoop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _test_with_delays(self, d13, d23):
        peer1_3, peer3_1 = create_handshaked_peer_pair(self.network)
        peer2_3, peer3_2 = create_handshaked_peer_pair(self.network)

        bcv_with_blocks = BlockChainView()
        bcv_with_blocks.do_headers_improve_path(self.BLOCK_LIST)

        bcv = BlockChainView()
        inv_batcher = InvBatcher()

        def set_callbacks(peer, delay):

            def getheaders_handler(peer, name, data):
                if len(data["hashes"]) == 1:
                    peer.send_msg("headers", headers=[(h, _) for _, h in enumerate(self.BLOCK_LIST[1:])])

            def getdata_handler(peer, name, data):
                for inv in data.get("items"):
                    if inv.item_type == ITEM_TYPE_BLOCK:
                        b = self.block_lookup[inv.data]
                        asyncio.get_event_loop().call_later(delay, lambda: peer.send_msg("block", block=b))

            peer.set_request_callback("getheaders", getheaders_handler)
            peer.set_request_callback("getdata", getdata_handler)
            peer.start()

        set_callbacks(peer1_3, d13)
        set_callbacks(peer2_3, d23)

        peer_q = asyncio.Queue()
        for p in [peer3_1, peer3_2]:
            run(inv_batcher.add_peer(p))
            run(peer_q.put(p))

        index_hash_work_tuples = []

        read_blocks = []
        for v in fetch_blocks_after(self.network, index_hash_work_tuples, peer_q):
            if v is None:
                break
            block, index = v
            read_blocks.append(block)
            if len(read_blocks) >= len(self.BLOCK_LIST):
                break

        for b1, b2 in zip(read_blocks, self.BLOCK_LIST):
            assert b1.id() == b2.id()

        for p in peer1_3, peer2_3, peer3_1, peer3_2:
            p.close()

    def test_1(self):
        self._test_with_delays(0, 0)

    @unittest.skip
    def test_Blockcatchup(self):
        for d13 in (0, 0.5, 5.0):
            for d23 in (0, 2.0, 10.0):
                self._test_with_delays(d13, d23)

    @unittest.skip
    def test_stupid_peer(self):
        # in this test, peer1 only responds to odd numbered blocks
        peer1_3, peer3_1 = create_handshaked_peer_pair(self.network)
        peer2_3, peer3_2 = create_handshaked_peer_pair(self.network)

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
