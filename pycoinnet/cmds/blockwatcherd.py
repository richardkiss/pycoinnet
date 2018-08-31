#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.encoding.hexbytes import b2h_rev
from pycoin.networks.registry import network_for_netcode

from pycoinnet.blockcatchup import fetch_blocks_after


from .common import (
    init_logging, storage_base_path, get_current_view, save_bcv, peer_manager_for_args
)


def flush_block_update(bcv, path, block_update):
    if not block_update:
        return
    block_index, block = block_update[0]
    logging.info("updating %d blocks starting at %d for path %s" % (len(block_update), block_index, path))
    block_list = [block for _, block in block_update]
    block_number = bcv.do_headers_improve_path(block_list)
    if block_number is not False:
        bcv.winnow()
        save_bcv(path, bcv)


def fetch_blocks(peer_manager, bcv, path, max_batch_size=1000):
    loop = asyncio.get_event_loop()
    filter_f = None

    block_update = []

    for block, block_index, in fetch_blocks_after(peer_manager, bcv.clone(), 8, filter_f=filter_f):
        block_update.append((block_index, block))
        if len(block_update) >= max_batch_size:
            loop.run_until_complete(loop.run_in_executor(None, flush_block_update, bcv, path, block_update))
            block_update = []

    loop.run_until_complete(loop.run_in_executor(None, flush_block_update, bcv, path, block_update))


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Watch Bitcoin network for new blocks.")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('-p', "--path", help='The path to the wallet files.')
    parser.add_argument(
        '-f', "--fast-forward", type=int,
        help="block index to fast-forward to (ie. don't download full blocks prior to this one)", default=0
    )
    parser.add_argument("--peer", metavar="peer_ip[/port]", help="Fetch from this peer.",
                        type=str, nargs="*")

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "blockwatcherd.json")

    bcv = get_current_view(path)

    peer_manager = peer_manager_for_args(args)
    asyncio.get_event_loop().run_until_complete(fetch_blocks(peer_manager, bcv, path))

    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
