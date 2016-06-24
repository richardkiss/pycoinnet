import asyncio
import logging

from tests.pipes import create_pipe_pairs
from tests.helper import make_blocks

from pycoinnet.networks import MAINNET
from pycoinnet.msg.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.PeerProtocol import PeerProtocol


def test_BlockHandler_tcp():
    
    loop = asyncio.get_event_loop()
    run = loop.run_until_complete
    peer1_3, peer3_1 = run(create_pipe_pairs(lambda: PeerProtocol(MAINNET)))
    peer2_3, peer3_2 = run(create_pipe_pairs(lambda: PeerProtocol(MAINNET)))

    BLOCK_LIST = make_blocks(128)
    block_lookup = {b.hash(): b for b in BLOCK_LIST}

    blockfetcher = Blockfetcher()
    blockfetcher.add_peer(peer3_1)
    blockfetcher.add_peer(peer3_2)

    @asyncio.coroutine
    def handle_getdata(peer):
        while True:
            try:
                msg, data = yield from peer.next_message()
            except Exception:
                break
            if msg == 'getdata':
                for inv in data.get("items"):
                    if inv.item_type == ITEM_TYPE_BLOCK:
                        b = block_lookup[inv.data]
                        peer.send_msg("block", block=b)

    loop.create_task(handle_getdata(peer1_3))
    loop.create_task(handle_getdata(peer2_3))

    @asyncio.coroutine
    def handle_getdata3(peer, bf):
        while True:
            try:
                msg, data = yield from peer.next_message()
            except Exception:
                break
            bf.handle_msg(msg, data)

    loop.create_task(handle_getdata3(peer3_1, blockfetcher))
    loop.create_task(handle_getdata3(peer3_2, blockfetcher))

    block_hash_priority_pair_list = [(b.hash(), idx) for idx, b in enumerate(BLOCK_LIST)]
    block_futures = blockfetcher.fetch_blocks(block_hash_priority_pair_list)

    for bf, block1 in zip(block_futures, BLOCK_LIST):
        block = run(bf)
        assert block.id() == block1.id()


asyncio.tasks._DEBUG = True
logging.basicConfig(
    level=logging.DEBUG,
    format=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s'))
logging.getLogger("asyncio").setLevel(logging.INFO)
