import argparse
import asyncio

from pycoin.encoding.hexbytes import h2b_rev
from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoin.networks.registry import network_for_netcode

from pycoinnet.cmds.common import init_logging, peer_manager_for_args
from pycoinnet.inv_batcher import InvBatcher


async def get_blocks(args):
    peer_manager = peer_manager_for_args(args)
    inv_batcher = InvBatcher(peer_manager)

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
    parser.add_argument("-p", "--peer", metavar="peer_ip[/port]", help="Fetch from this peer.",
                        type=str, nargs="*")
    parser.add_argument('id', nargs="+", help='Block ID as hex')

    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(get_blocks(args))


if __name__ == '__main__':
    main()
