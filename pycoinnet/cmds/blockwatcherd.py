#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.networks.registry import network_codes, network_for_netcode
from pycoin.serialize import b2h_rev

from pycoinnet.headerpipeline import improve_headers
from pycoinnet.MappingQueue import MappingQueue

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.version import NODE_NETWORK

from .common import (
    init_logging, storage_base_path, get_current_view, save_bcv, peer_connect_pipeline
)
from pycoinnet.pong_manager import install_pong_manager


def flush_block_update(bcv, path, block_update):
    if not block_update:
        return
    block_index, block = block_update[0]
    logging.info("updating %d blocks starting at %d for path %s" % (len(block_update), block_index, path))
    block_number = bcv.do_headers_improve_path([block for _, block in block_update])
    if block_number is not False:
        bcv.winnow()
        save_bcv(path, bcv)


async def fetch_blocks(bcv, network, path, max_batch_size=1000):
    loop = asyncio.get_event_loop()

    peers = set()

    block_fetcher = Blockfetcher()

    peer_q = peer_connect_pipeline(network)

    async def do_improve_headers(peer, q):
        if peer.version["services"] & NODE_NETWORK != 0:
            block_fetcher.add_peer(peer)
        peers.add(peer)
        install_pong_manager(peer)
        peer.start_dispatcher()
        await improve_headers(peer, bcv, q)

    async def handle_headers(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_hashes = item
        logging.info("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]
        block_futures = block_fetcher.fetch_blocks(block_hash_priority_pair_list)
        for _, bf in enumerate(block_futures):
            block = await bf
            await q.put((first_block_index + _, block))

    filters = [
        dict(callback_f=do_improve_headers, input_q=peer_q, worker_count=3),
        dict(callback_f=handle_headers, worker_count=1),
    ]
    q = MappingQueue(*filters)

    block_update = []
    while True:
        update = await q.get()
        print(update)
        if update is None:
            break
        block_index, block = update
        block_update.append((block_index, block))
        if len(block_update) >= max_batch_size:
            await loop.run_in_executor(None, flush_block_update, bcv, path, block_update)
        block_update = []

    await loop.run_in_executor(None, flush_block_update, bcv, path, block_update)
    block_update = []

    q.cancel()

    for peer in peers:
        peer.close()
    for peer in peers:
        await peer.wait_for_cleanup()
    loop.stop()


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

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "blockwatcherd.json")

    bcv = get_current_view(path)

    asyncio.get_event_loop().run_until_complete(fetch_blocks(bcv, args.network, path))

    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
