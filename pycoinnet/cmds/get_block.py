import argparse
import asyncio

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoin.serialize import h2b_rev

from pycoinnet.networks import MAINNET, TESTNET
from pycoinnet.cmds.common import init_logging, peer_connect_pipeline
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.version import NODE_NETWORK



async def set_up_inv_batcher(network):
    inv_batcher = InvBatcher()

    async def event_loop(peer):
        while True:
            name, data = await peer.next_message(unpack_to_dict=True)
            inv_batcher.handle_event(peer, name, data)
            if name == 'ping':
                peer.send_msg("pong", nonce=data["nonce"])
            if name == 'headers':
                await headers_msg_q.put((name, data))

    # add some peers to InvBatcher
    async def do_add_peer(peer, q):
        await inv_batcher.add_peer(peer)
        inv_batcher.task = asyncio.get_event_loop().create_task(event_loop(peer))

    inv_batcher.q = MappingQueue(dict(input_q=peer_connect_pipeline(network), callback_f=do_add_peer))
    return inv_batcher


async def get_blocks(args, network):
    inv_batcher = await set_up_inv_batcher(network)

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
    parser.add_argument('id', nargs="+", help='Block ID as hex')

    args = parser.parse_args()

    network = MAINNET

    block = asyncio.get_event_loop().run_until_complete(get_blocks(args, network))


if __name__ == '__main__':
    main()
