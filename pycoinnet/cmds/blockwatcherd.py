#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.serialize import b2h_rev


from pycoinnet.headerpipeline import improve_headers
from pycoinnet.networks import MAINNET
from pycoinnet.MappingQueue import MappingQueue

from pycoinnet.Blockfetcher import Blockfetcher

from .common import (
    init_logging, storage_base_path, get_current_view, save_bcv, install_pong_manager, peer_connect_pipeline
)


async def update_chain_state(network, bcv, update_q, count, add_peer_callback):

    peers = set()
    peer_q = peer_connect_pipeline(network)

    async def do_improve_headers(peer, q):
        add_peer_callback(peer)
        install_pong_manager(peer)
        peer.start_dispatcher()
        await improve_headers(peer, bcv, update_q)
        await q.put(peer)

    filters = [
        dict(callback_f=do_improve_headers, input_q=peer_q, worker_count=count),
    ]
    q = MappingQueue(*filters)

    for _ in range(count):
        peers.add(await q.get())
    return peers


async def handle_headers_q(block_fetcher, update_q, block_future_q):
    """
    This function takes update records out of update_q, unwraps them, then puts them into block_future_q
    """
    while 1:
        v = await update_q.get()
        if v is None:
            break
        first_block_index, block_hashes = v
        logging.info("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]
        block_futures = block_fetcher.fetch_blocks(block_hash_priority_pair_list)
        for _, bf in enumerate(block_futures):
            await block_future_q.put((first_block_index + _, bf))
    await block_future_q.put(None)


@asyncio.coroutine
def handle_update_q(bcv, path, block_future_q, max_batch_size):
    """
    This function takes block_future_q records and flushes them from time to time.
    """
    block_update = []
    loop = asyncio.get_event_loop()
    while 1:
        v = yield from block_future_q.get()
        if v is None:
            break
        block_index, bf = v
        block = yield from bf
        block_update.append((block_index, block))
        if len(block_update) >= max_batch_size:
            yield from loop.run_in_executor(None, flush_block_update, bcv, path, block_update)
    yield from loop.run_in_executor(None, flush_block_update, bcv, path, block_update)


def flush_block_update(bcv, path, block_update):
    if not block_update:
        return
    block_index, block = block_update[0]
    logging.info("updating %d blocks starting at %d for path %s" % (len(block_update), block_index, path))
    block_number = bcv.do_headers_improve_path([block for _, block in block_update])
    if block_number is not False:
        bcv.winnow()
        save_bcv(path, bcv)
    block_update[:] = []


async def fetch_blocks(bcv, network, path):
    loop = asyncio.get_event_loop()
    update_q = asyncio.Queue()
    peers = set()

    block_fetcher = Blockfetcher()
    block_future_q = asyncio.Queue(maxsize=1000)
    handle_headers_q_task = loop.create_task(handle_headers_q(block_fetcher, update_q, block_future_q))
    handle_update_q_task = loop.create_task(handle_update_q(bcv, path, block_future_q, max_batch_size=128))

    def add_peer(peer):
        block_fetcher.add_peer(peer)
        peers.add(peer)

    peers = await update_chain_state(network, bcv, update_q=update_q, count=3, add_peer_callback=add_peer)

    await handle_headers_q_task
    await handle_update_q_task

    for peer in peers:
        peer.close()
    for peer in peers:
        await peer.wait_for_cleanup()
    loop.stop()


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "blockwatcherd.json")

    bcv = get_current_view(path)
    network = MAINNET

    asyncio.get_event_loop().run_until_complete(fetch_blocks(bcv, network, path))

    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
