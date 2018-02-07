import argparse
import asyncio

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoin.serialize import h2b_rev

from pycoinnet.networks import MAINNET, TESTNET
from pycoinnet.cmds.common import init_logging, peer_connect_pipeline
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.version import NODE_NETWORK


async def get_block(block_hash, network):

    loop = asyncio.get_event_loop()

    async def fetch_block(peer, q):
        version = peer.version
        if version["services"] & NODE_NETWORK == 0:
            return
        peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, block_hash)])
        while True:
            name, data = await peer.next_message(unpack_to_dict=True)
            if name == 'ping':
                peer.send_msg("pong", nonce=data["nonce"])
            if name == 'block':
                block = data["block"]
                if block.hash() == block_hash:
                    await q.put(block)
                peer.close()
                break

    q = MappingQueue(dict(input_q=peer_connect_pipeline(network), callback_f=fetch_block, worker_count=2))
    block = await q.get()
    q.cancel()
    loop.stop()
    return block


def main():
    init_logging(debug=False)
    parser = argparse.ArgumentParser(description="Fetch a block by ID.")
    parser.add_argument('id', help='Block ID as hex')

    args = parser.parse_args()

    network = MAINNET

    block = asyncio.get_event_loop().run_until_complete(get_block(h2b_rev(args.id), network))

    with open("%s.bin" % block.id(), "wb") as f:
        block.stream(f)
    print(block.id())


if __name__ == '__main__':
    main()
