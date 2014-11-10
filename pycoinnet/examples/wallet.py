#!/usr/bin/env python

import argparse
import asyncio
import calendar
import datetime
import os.path
import sqlite3
import time

from pycoinnet.helpers.networks import MAINNET

from pycoinnet.util.BlockChainView import BlockChainView

from pycoinnet.examples.spvclient import SPVClient
from pycoinnet.bloom import BloomFilter, filter_size_required, hash_function_count_required


from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
from pycoin.wallet.SQLite3Wallet import SQLite3Wallet


def storage_base_path():
    p = os.path.expanduser("~/.wallet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


class Keychain(object):
    def __init__(self, addresses):
        self.interested_addresses = set(addresses)

    def is_spendable_interesting(self, spendable):
        return spendable.bitcoin_address() in self.interested_addresses


def main():
    parser = argparse.ArgumentParser(description="SPV wallet.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')
    parser.add_argument('-d', "--date", help="Skip ahead to this date.",
                        type=lambda x: time.strptime(x, '%Y-%m-%d'),
                        default=datetime.date(2008, 1, 1))

    args = parser.parse_args()

    path = args.path or storage_base_path()

    early_timestamp = calendar.timegm(args.date)

    print(path)
    print("wallet. Fetching.")
    network = MAINNET

    addresses = [a[:-1] for a in open(os.path.join(path, "watch_addresses")).readlines()]

    keychain = Keychain(addresses)

    sql_db = sqlite3.Connection(os.path.join(path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)
    wallet = SQLite3Wallet(keychain, persistence, desired_spendable_count=20)

    bcv_json = persistence.get_global("blockchain_view") or "[]"

    blockchain_view = BlockChainView.from_json(bcv_json)

    element_count = len(addresses)
    false_positive_probability = 0.0001

    filter_size = filter_size_required(element_count, false_positive_probability)
    hash_function_count = hash_function_count_required(filter_size, element_count)
    bloom_filter = BloomFilter(filter_size, hash_function_count=hash_function_count, tweak=1)

    print("%d elements; filter size: %d bytes; %d hash functions" % (element_count, filter_size, hash_function_count))

    for a in addresses:
        bloom_filter.add_address(a)

    merkle_block_index_queue = asyncio.Queue()
    host_port_q = None
    USE_LOCAL_HOST = False
    if USE_LOCAL_HOST:
        # use a local host instead of going to DNS
        host_port_q = asyncio.Queue()
        host_port_q.put_nowait(("127.0.0.1", 8333))
    filter_f = lambda idx, h: h.timestamp >= early_timestamp
    spv = SPVClient(
        network, blockchain_view, bloom_filter, merkle_block_index_queue, host_port_q=host_port_q,
        filter_f=filter_f)

    @asyncio.coroutine
    def process_updates(merkle_block_index_queue):
        while True:
            merkle_block, index = yield from merkle_block_index_queue.get()
            wallet._add_block(merkle_block, index, merkle_block.txs)
            bcv_json = blockchain_view.as_json()
            persistence.set_global("blockchain_view", bcv_json)
            if len(merkle_block.txs) > 0:
                print("got block %06d: %s... with %d transactions" % (
                    index, merkle_block.id()[:32], len(merkle_block.txs)))
            if index % 1000 == 0:
                print("at block %06d (%s)" % (index, datetime.datetime.fromtimestamp(merkle_block.timestamp)))
            if merkle_block_index_queue.empty():
                persistence.commit()

    t = asyncio.Task(process_updates(merkle_block_index_queue))
    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
