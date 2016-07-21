#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path
import sys

from pycoin.serialize import b2h_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.headerpipeline import improve_headers
from pycoinnet.networks import MAINNET, TESTNET

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.Peer import Peer

from pycoinnet.scripts.common import (
    init_logging, storage_base_path, get_current_view, save_bcv, VERSION_MSG
)


@asyncio.coroutine
def update_headers(network, q, bcv, update_q, peer_created_callback):
    while 1:
        peer_addr = yield from q.get()
        if peer_addr is None:
            return
        host, port = peer_addr
        logging.info("connecting to %s:%d", host, port)
        reader, writer = yield from asyncio.open_connection(host=host, port=port)
        break

    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    yield from peer.perform_handshake(**VERSION_MSG)
    peer_created_callback(peer)
    peer.start_dispatcher()
    yield from improve_headers(peer, bcv, update_q)
    bcv.winnow()


@asyncio.coroutine
def update_headers_pipeline(network, bcv, count, update_q, peer_created_callback):
    futures = []
    q = dns_bootstrap_host_port_q(network)
    bcv_copy = bcv.clone()
    for _ in range(count):
        futures.append(
            update_headers(network, q, bcv_copy, update_q, peer_created_callback))
    yield from asyncio.wait(futures)
    update_q.put_nowait(None)


@asyncio.coroutine
def handle_headers_q(block_fetcher, update_q, block_future_q):
    while 1:
        v = yield from update_q.get()
        if v is None:
            break
        first_block_index, block_hashes = v
        logging.info("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]
        block_futures = block_fetcher.fetch_blocks(block_hash_priority_pair_list)
        for _, bf in enumerate(block_futures):
            yield from block_future_q.put((first_block_index + _, bf))
    yield from block_future_q.put(None)


@asyncio.coroutine
def handle_update_q(bcv, path, block_future_q, max_batch_size):
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


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "blockwatcherd.json")

    loop = asyncio.get_event_loop()
    update_q = asyncio.Queue()
    bcv = get_current_view(path)
    network = MAINNET
    peers = set()

    block_fetcher = Blockfetcher()
    block_future_q = asyncio.Queue(maxsize=1000)
    handle_headers_q_task = loop.create_task(handle_headers_q(block_fetcher, update_q, block_future_q))
    handle_update_q_task = loop.create_task(handle_update_q(bcv, path, block_future_q, max_batch_size=128))

    def add_peer(peer):
        block_fetcher.add_peer(peer)
        peers.add(peer)

    loop.run_until_complete(update_headers_pipeline(
        network, bcv, count=2, update_q=update_q, peer_created_callback=add_peer))

    loop.run_until_complete(handle_headers_q_task)
    loop.run_until_complete(handle_update_q_task)

    for peer in peers:
        peer.close()
    for peer in peers:
        loop.run_until_complete(peer.wait_for_cleanup())
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
