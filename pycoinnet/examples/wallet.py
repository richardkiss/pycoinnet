#!/usr/bin/env python

import argparse
import asyncio
import calendar
import codecs
import datetime
import io
import os.path
import sqlite3
import time

from pycoin.convention import satoshi_to_mbtc
from pycoin.key.validate import is_address_valid
from pycoin.tx import Tx
from pycoin.tx.tx_utils import create_tx
from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
from pycoin.wallet.SQLite3Wallet import SQLite3Wallet

from pycoinnet.bloom import BloomFilter, filter_size_required, hash_function_count_required
from pycoinnet.examples.spvclient import SPVClient
from pycoinnet.helpers.networks import MAINNET
from pycoinnet.util.BlockChainView import BlockChainView


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


def wallet_fetch(path, args):
    early_timestamp = calendar.timegm(args.date)

    print(path)
    print("wallet. Fetching.")
    network = MAINNET

    addresses = [a[:-1] for a in open(os.path.join(path, "watch_addresses")).readlines()]

    keychain = Keychain(addresses)

    sql_db = sqlite3.Connection(os.path.join(path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)
    wallet = SQLite3Wallet(keychain, persistence)

    bcv_json = persistence.get_global("blockchain_view") or "[]"

    blockchain_view = BlockChainView.from_json(bcv_json)
    if args.rewind:
        print("rewinding to block %d" % args.rewind)
        blockchain_view.rewind(args.rewind)

    spendables = list(persistence.unspent_spendables(blockchain_view.last_block_index()))

    element_count = len(addresses) + len(spendables)
    false_positive_probability = 0.00001

    filter_size = filter_size_required(element_count, false_positive_probability)
    hash_function_count = hash_function_count_required(filter_size, element_count)
    bloom_filter = BloomFilter(filter_size, hash_function_count=hash_function_count, tweak=1)

    print("%d elements; filter size: %d bytes; %d hash functions" % (
            element_count, filter_size, hash_function_count))

    for a in addresses:
        bloom_filter.add_address(a)

    for s in spendables:
        bloom_filter.add_spendable(s)

    merkle_block_index_queue = asyncio.Queue()
    host_port_q = None
    USE_LOCAL_HOST = False
    if USE_LOCAL_HOST:
        # use a local host instead of going to DNS
        host_port_q = asyncio.Queue()
        host_port_q.put_nowait(("127.0.0.1", 8333))

    def filter_f(idx, h):
        return h.timestamp >= early_timestamp

    # this spv value probably needs to be kept on the stack so the SPVClient is not GCed
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
                print("at block %06d (%s)" % (
                        index, datetime.datetime.fromtimestamp(merkle_block.timestamp)))
            if merkle_block_index_queue.empty():
                persistence.commit()

    # we need to keep the task around in the stack context or it will be GCed
    t = asyncio.Task(process_updates(merkle_block_index_queue))
    asyncio.get_event_loop().run_forever()


def wallet_balance(path, args):
    sql_db = sqlite3.Connection(os.path.join(path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)
    bcv_json = persistence.get_global("blockchain_view") or "[]"
    blockchain_view = BlockChainView.from_json(bcv_json)
    last_block = blockchain_view.last_block_index()
    total = 0
    for spendable in persistence.unspent_spendables(last_block, confirmations=1):
        total += spendable.coin_value
    print("block %d: balance = %s mBTC" % (last_block, satoshi_to_mbtc(total)))


def as_payable(payable):
    address, amount = payable, None
    if "/" in payable:
        address, amount = payable.split("/", 1)
    if not is_address_valid(address):
        raise argparse.ArgumentTypeError("%s is not a valid address" % address)
    if amount is not None:
        return (address, int(amount))
    return address


def wallet_create(path, args):
    sql_db = sqlite3.Connection(os.path.join(path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)

    bcv_json = persistence.get_global("blockchain_view") or "[]"
    blockchain_view = BlockChainView.from_json(bcv_json)
    last_block = blockchain_view.last_block_index()

    # how much are we sending?
    total_sending = 0
    for p in args.payable:
        if len(p) == 2:
            total_sending += p[-1]

    if total_sending == 0:
        raise argparse.ArgumentTypeError("you must choose a non-zero amount to send")

    total = 0
    spendables = []
    for spendable in persistence.unspent_spendables(last_block, confirmations=1):
        spendables.append(spendable)
        total += spendable.coin_value
        if total >= total_sending:
            break

    print("found %d coins which exceed %d" % (total, total_sending))

    tx = create_tx(spendables, args.payable)
    with open(args.output, "wb") as f:
        tx.stream(f)
        tx.stream_unspents(f)


def wallet_exclude(path, args):
    sql_db = sqlite3.Connection(os.path.join(path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)

    with open(args.path_to_tx, "rb") as f:
        if f.name.endswith("hex"):
            f = io.BytesIO(codecs.getreader("hex_codec")(f).read())
        tx = Tx.parse(f)

    for tx_in in tx.txs_in:
        spendable = persistence.spendable_for_hash_index(tx_in.previous_hash, tx_in.previous_index)
        if spendable:
            spendable.does_seem_spent = True
            persistence.save_spendable(spendable)
    persistence.commit()


def main():
    parser = argparse.ArgumentParser(description="SPV wallet.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')
    subparsers = parser.add_subparsers(help="commands", dest='command')

    fetch_parser = subparsers.add_parser('fetch', help='Update to current blockchain view')
    fetch_parser.add_argument('-d', "--date", help="Skip ahead to this date.",
                              type=lambda x: time.strptime(x, '%Y-%m-%d'),
                              default=time.strptime('2008-01-01', '%Y-%m-%d'))

    fetch_parser.add_argument('-r', "--rewind", help="Rewind to this block index.", type=int)

    subparsers.add_parser('balance', help='Show wallet balance')

    create_parser = subparsers.add_parser('create', help='Create transaction')
    create_parser.add_argument("-o", "--output", type=str, help="name of tx output file", required=True)
    create_parser.add_argument('payable', type=as_payable, nargs='+',
                               help="payable: either a bitcoin address, or a address/amount combo")

    exclude_parser = subparsers.add_parser('exclude', help="Exclude spendables from a given transaction")
    exclude_parser.add_argument('path_to_tx', help="path to transaction")

    args = parser.parse_args()
    path = args.path or storage_base_path()

    if args.command == "fetch":
        wallet_fetch(path, args)
    if args.command == "balance":
        wallet_balance(path, args)
    if args.command == "create":
        wallet_create(path, args)
    if args.command == "exclude":
        wallet_exclude(path, args)


if __name__ == '__main__':
    main()
