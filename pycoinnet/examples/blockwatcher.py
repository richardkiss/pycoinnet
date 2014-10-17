#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import logging
import os

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
        while block_q.qsize() > depth:
            # we have blocks that are buried and ready to write
            future, block_hash, block_index = yield from block_q.get()
            block = yield from asyncio.wait_for(future, timeout=None)
            write_block_to_disk(blockdir, block, block_index)
            update_last_processed_block(state_dir, block_index)


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
        help="Minimum depth blocks must be buried before being dropped in blockdir", default=1
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

    client = Client(
        network, host_port_q, should_download_block_f, block_chain_store, do_update)

    blockfetcher = client.blockfetcher

    block_processor_task = asyncio.Task(
        block_processor(
            change_q, blockfetcher, state_dir, args.blockdir, args.depth))

    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
