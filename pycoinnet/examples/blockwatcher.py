#!/usr/bin/env python

"""
Custom bitcoin client
"""

import argparse
import asyncio
import logging
import os

from pycoinnet.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.util.BlockChainStore import BlockChainStore

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.Fetcher import Fetcher

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.BlockHandler import BlockHandler
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.helpers.networks import MAINNET
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_pingpong_manager
from pycoinnet.helpers.standards import manage_connection_count
from pycoinnet.helpers.standards import version_data_for_peer
from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q

from pycoinnet.util.TwoLevelDict import TwoLevelDict

from pycoinnet.PeerAddress import PeerAddress


@asyncio.coroutine
def show_connection_info(connection_info_q):
    while True:
        verb, noun, peer = yield from connection_info_q.get()
        logging.info("connection manager: %s on %s", verb, noun)


def write_block_to_disk(blockdir, block, block_index):
    p = os.path.join(blockdir, "block-%06d-%s.bin" % (block_index, block.id()))
    tmp_path = p + ".tmp"
    with open(tmp_path, "wb") as f:
        block.stream(f)
    os.rename(tmp_path, p)

def update_last_processed_block(config_dir, last_processed_block):
    last_processed_block_path = os.path.join(config_dir, "last_processed_block")
    try:
        with open(last_processed_block_path, "w") as f:
            f.write("%d\n" % last_processed_block)
            f.close()
    except Exception:
        logging.exception("problem writing %s", last_processed_block_path)

def get_last_processed_block(config_dir):
    last_processed_block_path = os.path.join(config_dir, "last_processed_block")
    try:
        with open(last_processed_block_path) as f:
            last_processed_block = int(f.readline()[:-1])
    except Exception:
        logging.exception("problem getting last processed block, using 0")
        last_processed_block = 0
    return last_processed_block

def block_processor(change_q, blockfetcher, config_dir, blockdir, depth):
    last_processed_block = get_last_processed_block(config_dir)
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
            update_last_processed_block(config_dir, block_index)

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

def block_chain_locker(block_chain):
    @asyncio.coroutine
    def _run(block_chain, change_q):
        LOCKED_MULTIPLE = 32
        while True:
            total_length = block_chain.length()
            locked_length = block_chain.locked_length()
            unlocked_length = total_length - locked_length
            if unlocked_length > LOCKED_MULTIPLE:
                new_locked_length = total_length - (total_length % LOCKED_MULTIPLE) - LOCKED_MULTIPLE
                block_chain.lock_to_index(new_locked_length)
            # wait for a change to blockchain
            op, block_header, block_index = yield from change_q.get()

    return asyncio.Task(_run(block_chain, block_chain.new_change_q()))

@asyncio.coroutine
def new_block_fetcher(inv_collector, block_chain):
    item_q = inv_collector.new_inv_item_queue()
    while True:
        inv_item = yield from item_q.get()
        if inv_item.item_type == ITEM_TYPE_BLOCK:
            if not block_chain.is_hash_known(inv_item.data):
                block = yield from inv_collector.fetch(inv_item)
                block_chain.add_headers([block])

LOG_FORMAT=('%(asctime)s [%(process)d] [%(levelname)s] '
            '%(filename)s:%(lineno)d %(message)s')

def log_file(logPath, level=logging.NOTSET):
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)

def main():
    parser = argparse.ArgumentParser(description="Watch Bitcoin network for new blocks.")
    parser.add_argument('-c', "--config-dir", help='The directory where config files are stored.')
    parser.add_argument(
        '-f', "--fast-forward", type=int,
        help="block index to fast-forward to (ie. don't download full blocks prior to this one)", default=0
    )
    parser.add_argument(
        '-d', "--depth", type=int,
        help="Minimum depth blocks must be buried before being dropped in blockdir", default=2
    )
    parser.add_argument( '-l', "--log-file", help="Path to log file", default=None)
    parser.add_argument("blockdir", help='The directory where new blocks are dropped.')

    asyncio.tasks._DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.INFO)

    if 1:
        host_port_q = dns_bootstrap_host_port_q(MAINNET)
    else:
        host_port_q = asyncio.Queue()
        host_port_q.put_nowait(("127.0.0.1", 8333))

    args = parser.parse_args()

    if args.log_file:
        log_file(args.log_file)

    block_chain_store = BlockChainStore(args.config_dir)
    block_chain = BlockChain(did_lock_to_index_f=block_chain_store.did_lock_to_index)

    locker_task = block_chain_locker(block_chain)
    block_chain.add_nodes(block_chain_store.block_tuple_iterator())

    blockfetcher = Blockfetcher()
    inv_collector = InvCollector()

    block_store = TwoLevelDict()

    @asyncio.coroutine
    def _rotate(block_store):
        while True:
            block_store.rotate()
            yield from asyncio.sleep(1800)
    rotate_task = asyncio.Task(_rotate(block_store))

    blockhandler = BlockHandler(inv_collector, block_chain, block_store,
        should_download_f=lambda block_hash, block_index: block_index >= args.fast_forward)

    last_processed_block = max(get_last_processed_block(config_dir), args.fast_forward)
    update_last_processed_block(config_dir, last_processed_block)

    change_q = asyncio.Queue()
    from pycoinnet.util.BlockChain import _update_q
    block_chain.add_change_callback(lambda blockchain, ops: _update_q(change_q, ops))

    block_processor_task = asyncio.Task(
        block_processor(
            change_q, blockfetcher, args.config_dir, args.blockdir, args.depth)))

    fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)

    fetcher_task = asyncio.Task(new_block_fetcher(inv_collector, block_chain))

    def create_protocol_callback():
        peer = BitcoinPeerProtocol(MAINNET["MAGIC_HEADER"])
        install_pingpong_manager(peer)
        fetcher = Fetcher(peer)
        peer.add_task(run_peer(
            peer, fetcher, fast_forward_add_peer,
            blockfetcher, inv_collector, blockhandler))
        return peer

    connection_info_q = manage_connection_count(host_port_q, create_protocol_callback, 8)
    show_task = asyncio.Task(show_connection_info(connection_info_q))
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
