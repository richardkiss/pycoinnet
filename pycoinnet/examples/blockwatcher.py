#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import logging
import os
import time

from pycoinnet.examples.Client import Client

from pycoinnet.util.BlockChainStore import BlockChainStore

from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.networks import MAINNET, TESTNET


def write_block_to_disk(blockdir, block, block_index):
    p = os.path.join(blockdir, "block-%06d-%s.bin" % (block_index, block.id()))
    tmp_path = p + ".tmp"
    with open(tmp_path, "wb") as f:
        block.stream(f)
    os.rename(tmp_path, p)


def update_last_processed_block(state_dir, last_processed_block):
    last_processed_block_path = os.path.join(state_dir, "last_processed_block")
    try:
        with open(last_processed_block_path, "w") as f:
            f.write("%d\n" % last_processed_block)
            f.close()
    except Exception:
        logging.exception("problem writing %s", last_processed_block_path)


def get_last_processed_block(state_dir):
    last_processed_block_path = os.path.join(state_dir, "last_processed_block")
    try:
        with open(last_processed_block_path) as f:
            last_processed_block = int(f.readline()[:-1])
    except Exception:
        logging.exception("problem getting last processed block, using 0")
        last_processed_block = 0
    return last_processed_block


def block_processor(change_q, blockfetcher, state_dir, blockdir, depth):
    last_processed_block = get_last_processed_block(state_dir)
    block_q = asyncio.Queue()
    while True:
        add_remove, block_hash, block_index = yield from change_q.get()
        if add_remove == "remove":
            the_other = block_q.pop()
            if the_other[1:] != (block_hash, block_index):
                logging.fatal("problem merging! did the block chain fork? %s %s", the_other, block_hash)
                import sys
                sys.exit(-1)
            continue
        if add_remove != "add":
            logging.error("something weird from change_q")
            continue
        if block_index < last_processed_block:
            continue
        item = (blockfetcher.get_block_future(block_hash, block_index), block_hash, block_index)
        block_q.put_nowait(item)
        if change_q.qsize() > 0:
            continue
        if block_q.qsize() <= depth:
            continue

        # create the time directory
        while 1:
            time_dir = str(int(time.time()))
            timed_blockdir = os.path.join(blockdir, time_dir)
            tmp_timed_blockdir = timed_blockdir + ".tmp"
            if not os.path.exists(timed_blockdir) and not os.path.exists(tmp_timed_blockdir):
                break
            yield from asyncio.sleep(1)
        os.mkdir(tmp_timed_blockdir)

        writ_count = 0
        while block_q.qsize() > depth:
            # we have blocks that are buried and ready to write
            future, block_hash, block_index = yield from block_q.get()
            block = yield from asyncio.wait_for(future, timeout=None)
            write_block_to_disk(tmp_timed_blockdir, block, block_index)
            update_last_processed_block(state_dir, block_index)
            writ_count += 1
            if writ_count >= 100:
                break
        os.rename(tmp_timed_blockdir, timed_blockdir)

@asyncio.coroutine
def run_peer(peer, fetcher, fast_forward_add_peer, blockfetcher, inv_collector, blockhandler):
    yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
    version_parameters = version_data_for_peer(peer)
    version_data = yield from initial_handshake(peer, version_parameters)
    last_block_index = version_data["last_block_index"]
    fast_forward_add_peer(peer, last_block_index)
    blockfetcher.add_peer(peer, fetcher, last_block_index)
    inv_collector.add_peer(peer)
    blockhandler.add_peer(peer)

def block_chain_locker_callback(block_chain, ops):
    LOCKED_MULTIPLE = 32
    total_length = block_chain.length()
    locked_length = block_chain.locked_length()
    unlocked_length = total_length - locked_length
    if unlocked_length > LOCKED_MULTIPLE:
        new_locked_length = total_length - (total_length % LOCKED_MULTIPLE) - LOCKED_MULTIPLE
        block_chain.lock_to_index(new_locked_length)

@asyncio.coroutine
def new_block_fetcher(inv_collector, block_chain):
    item_q = inv_collector.new_inv_item_queue()
    while True:
        inv_item = yield from item_q.get()
        if inv_item.item_type == ITEM_TYPE_BLOCK:
            if not block_chain.is_hash_known(inv_item.data):
                block = yield from inv_collector.fetch(inv_item)
                block_chain.add_headers([block])


LOG_FORMAT = ('%(asctime)s [%(process)d] [%(levelname)s] '
              '%(filename)s:%(lineno)d %(message)s')

def log_file(logPath, level=logging.NOTSET):
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)


def main():
    parser = argparse.ArgumentParser(description="Watch Bitcoin network for new blocks.")
    parser.add_argument('-s', "--state-dir", help='The directory where state files are stored.')
    parser.add_argument(
        '-f', "--fast-forward", type=int,
        help="block index to fast-forward to (ie. don't download full blocks prior to this one)", default=0
    )
    parser.add_argument(
        '-d', "--depth", type=int,
        help="Minimum depth blocks must be buried before being dropped in blockdir", default=0
    )
    parser.add_argument('-l', "--log-file", help="Path to log file", default=None)
    parser.add_argument("blockdir", help='The directory where new blocks are dropped.')

    args = parser.parse_args()

    asyncio.tasks._DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.INFO)

    if args.log_file:
        log_file(args.log_file)

    state_dir = args.state_dir
    block_chain_store = BlockChainStore(state_dir)
    block_chain = BlockChain(did_lock_to_index_f=block_chain_store.did_lock_to_index)

    block_chain.add_change_callback(block_chain_locker_callback)

    blockfetcher = Blockfetcher()
    inv_collector = InvCollector()

    network = MAINNET

    if 1:
        host_port_q = dns_bootstrap_host_port_q(network)
    else:
        host_port_q = asyncio.Queue()
        host_port_q.put_nowait(("127.0.0.1", 8333))

    should_download_block_f = lambda block_hash, block_index: block_index >= args.fast_forward

    last_processed_block = max(get_last_processed_block(state_dir), args.fast_forward)
    update_last_processed_block(state_dir, last_processed_block)

    change_q = asyncio.Queue()
    from pycoin.blockchain.BlockChain import _update_q

    def do_update(blockchain, ops):
        _update_q(change_q, [list(o) for o in ops])

block_chain.add_change_callback(do_update)
    block_chain.add_nodes(block_chain_store.block_tuple_iterator())

    block_processor_task = asyncio.Task(
        block_processor(
            change_q, blockfetcher, state_dir, args.blockdir, args.depth))

    client = Client(
        network, host_port_q, should_download_block_f, block_chain_store, do_update)

    blockfetcher = client.blockfetcher

    block_processor_task = asyncio.Task(
        block_processor(
            change_q, blockfetcher, state_dir, args.blockdir, args.depth))

    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
