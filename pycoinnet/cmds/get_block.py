import argparse
import asyncio

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoin.networks.registry import network_codes, network_for_netcode
from pycoin.serialize import h2b_rev

from pycoinnet.cmds.common import init_logging, peer_connect_pipeline
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.pong_manager import install_pong_manager
from pycoinnet.version import NODE_NETWORK


async def set_up_inv_batcher(network, max_peer_count=8):
    inv_batcher = InvBatcher()

    # add some peers to InvBatcher
    async def do_add_peer(peer, q):
        install_pong_manager(peer)
        version = peer.version
        if version["services"] & NODE_NETWORK == 0:
            peer.close()
            return
        await inv_batcher.add_peer(peer)
        peer.start()
        nonlocal max_peer_count
        max_peer_count -= 1
        if max_peer_count <= 0:
            inv_batcher.q.cancel()

    inv_batcher.q = MappingQueue(dict(input_q=peer_connect_pipeline(network), callback_f=do_add_peer))
    return inv_batcher


async def get_blocks(args):
    inv_batcher = await set_up_inv_batcher(args.network)

    block_futures = []
    for _ in args.id:
        f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, h2b_rev(_)))
        block_futures.append(f)

    for f in block_futures:
        block = await f
        with open("%s.bin" % block.id(), "wb") as f:
            block.stream(f)
        print(block.id())


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Fetch a block by ID.")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('id', nargs="+", help='Block ID as hex')

    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(get_blocks(args))


if __name__ == '__main__':
    main()
