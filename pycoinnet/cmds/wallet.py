#!/usr/bin/env python

import argparse
import asyncio
import calendar
import codecs
import datetime
import logging
import io
import os.path
import sqlite3
import time

from collections import defaultdict

from pycoin.bloomfilter import BloomFilter, filter_size_required, hash_function_count_required
from pycoin.convention import satoshi_to_mbtc
from pycoin.encoding.b58 import a2b_hashed_base58
from pycoin.ui.validate import is_address_valid
from pycoin.networks.registry import network_for_netcode
from pycoin.message.InvItem import ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK
from pycoin.serialize import b2h_rev
from pycoin.tx.tx_utils import create_tx
from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
from pycoin.wallet.SQLite3Wallet import SQLite3Wallet

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.blockcatchup import fetch_blocks_after

from .common import init_logging


class Keychain(object):
    def __init__(self, hash160_set, persistence, network):
        self._hash160_set = set(hash160_set)
        self._persistence = persistence
        self._network = network

    def is_spendable_interesting(self, spendable):
        for opcode, data, pc, new_pc in self._network.extras.ScriptTools.get_opcodes(spendable.script):
            if data in self._hash160_set:
                return True
        return False

    def hash160_set(self):
        return self._hash160_set


def bloom_filter_from_parameters(element_count, false_positive_probability, tweak=1):
    filter_size = filter_size_required(element_count, false_positive_probability)
    hash_function_count = hash_function_count_required(filter_size, element_count)
    bloom_filter = BloomFilter(filter_size, hash_function_count=hash_function_count, tweak=1)
    print("%d elements; filter size: %d bytes; %d hash functions" % (
            element_count, filter_size, hash_function_count))
    return bloom_filter


def bloom_filter_for_addresses_spendables(
        addresses, spendables, element_pad_count=0, false_positive_probability=0.000001):
    element_count = len(addresses) + len(spendables) + element_pad_count
    bloom_filter = bloom_filter_from_parameters(element_count, false_positive_probability)
    for a in addresses:
        bloom_filter.add_hash160(a)
    for s in spendables:
        bloom_filter.add_spendable(s)
    return bloom_filter


def basepath_persistence_for_args(args):
    basepath = os.path.join(os.path.expanduser(args.path), args.wallet_name, args.network.code)
    if not os.path.exists(basepath):
        os.makedirs(basepath)

    sql_db = sqlite3.Connection(os.path.join(basepath, "wallet.db"))
    return basepath, SQLite3Persistence(sql_db)


def wallet_persistence_for_args(args):
    basepath, persistence = basepath_persistence_for_args(args)

    hash160_list = [a2b_hashed_base58(a[:-1])[1:] for a in open(
        os.path.join(basepath, "watch_addresses")).readlines()]
    keychain = Keychain(hash160_list, persistence, args.network)

    wallet = SQLite3Wallet(keychain, persistence)
    bcv_json = persistence.get_global("blockchain_view") or "[]"
    bcv = BlockChainView.from_json(bcv_json)
    return wallet, persistence, bcv


def commit_to_persistence(blockchain_view, persistence, last_block=None):
    if last_block:
        blockchain_view.winnow(last_block)
    bcv_json = blockchain_view.as_json()
    persistence.set_global("blockchain_view", bcv_json)
    persistence.commit()


def wallet_fetch(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

    last_block = wallet.last_block_index()
    blockchain_view.rewind(last_block)
    wallet.rewind(last_block)

    spendables = list(persistence.unspent_spendables(last_block))

    bloom_filter = bloom_filter_for_addresses_spendables(
        wallet.keychain.hash160_set(), spendables, element_pad_count=2000)

    early_timestamp = calendar.timegm(args.date)

    def filter_f(bh, pri):
        if bh.timestamp >= early_timestamp:
            return ITEM_TYPE_BLOCK
            return ITEM_TYPE_MERKLEBLOCK

    filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
    flags = 1  # BLOOM_UPDATE_ALL = 1  # BRAIN DAMAGE

    async def new_peer_callback(peer):
        peer.send_msg("filterload", filter=filter_bytes, tweak=tweak,
                      hash_function_count=hash_function_count, flags=flags)

    index_hash_work_tuples = blockchain_view.node_tuples

    for block, index in fetch_blocks_after(
            args.network, index_hash_work_tuples, peer_addresses=args.peer,
            filter_f=filter_f, new_peer_callback=new_peer_callback):
        logging.debug("last_block_index = %s (%s)", index,
                      datetime.datetime.fromtimestamp(block.timestamp))
        txs = block.txs
        if len(txs) > 0:
            logging.info(
                "got block %06d: %s... with %d transactions",
                index, block.id()[:32], len(txs))
        wallet._add_block(block, index, txs)
        if index % 50 == 0:
            logging.info("at block %06d (%s)" % (
                index, datetime.datetime.fromtimestamp(block.timestamp)))
            wallet.set_last_block_index(index)
            commit_to_persistence(blockchain_view, persistence, index)


def wallet_balance(args):
    basepath, persistence = basepath_persistence_for_args(args)
    last_block = args.block or int(persistence.get_global("block_index") or 0)
    total = 0
    for spendable in persistence.unspent_spendables(last_block, confirmations=1):
        if 0 < spendable.block_index_available <= last_block:
            total += spendable.coin_value
    print("block %d: balance = %s mBTC" % (last_block, satoshi_to_mbtc(total)))


def as_payable(payable, network):
    address, amount = payable, None
    if "/" in payable:
        address, amount = payable.split("/", 1)
        amount = int(amount)
    if not is_address_valid(address, allowable_netcodes=[network.code]):
        raise argparse.ArgumentTypeError("%s is not a valid address" % address)
    if amount:
        return (address, amount)
    return address


def wallet_tx(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

    fee = args.fee

    last_block = blockchain_view.last_block_index()

    # how much are we sending?
    total_sending = 0
    payables = [as_payable(_, args.network) for _ in args.payable]
    for p in payables:
        if len(p) == 2:
            total_sending += p[-1]

    if total_sending == 0:
        raise argparse.ArgumentTypeError("you must choose a non-zero amount to send")

    total_input_value = 0
    spendables = []
    unspents = persistence.unspent_spendables(last_block, confirmations=1)

    for spendable in unspents:
        spendables.append(spendable)
        total_input_value += spendable.coin_value
        if total_input_value >= total_sending:
            break

    spendables.sort(key=lambda _: _.coin_value)
    print("found %d coins which exceed %d" % (total_input_value, total_sending))

    #change_amount = total_input_value - fee - total_sending
    #if change_amount > 0:
    #    change_address = keychain.get_change_address()
    #    payables.append(change_address)

    tx = create_tx(spendables, payables, network=args.network, fee=fee)
    with open(args.output, "wb") as f:
        tx.stream(f)
        tx.stream_unspents(f)
    print("wrote transaction to %s" % args.output)


def wallet_exclude(args):
    basepath, persistence = basepath_persistence_for_args(args)

    with open(args.path_to_tx, "rb") as f:
        if f.name.endswith("hex"):
            f = io.BytesIO(codecs.getreader("hex_codec")(f).read())
        tx = args.network.tx.parse(f)

    for tx_in in tx.txs_in:
        spendable = persistence.spendable_for_hash_index(tx_in.previous_hash, tx_in.previous_index)
        if spendable:
            spendable.does_seem_spent = True
            persistence.save_spendable(spendable)
    persistence.commit()


def wallet_rewind(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

    last_block_index = min(int(persistence.get_global("block_index") or 0), args.block_number)

    blockchain_view.rewind(last_block_index)
    wallet.rewind(last_block_index)

    bcv_json = blockchain_view.as_json()
    persistence.set_global("blockchain_view", bcv_json)
    persistence.commit()
    print("rewinding to block %d" % last_block_index)


def wallet_dump(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)
    lbi = wallet.last_block_index()

    for spendable in persistence.unspent_spendables(lbi, confirmations=1):
        if spendable.block_index_available != 0:
            print(spendable.as_text())


def satoshis_to_amount(s):
    if s >= 0:
        return "+%s " % satoshi_to_mbtc(s)
    return "(%s)" % satoshi_to_mbtc(s)


def wallet_history(args):
    basepath, persistence = basepath_persistence_for_args(args)
    spendables = list(persistence.all_spendables())
    spendable_lookup = defaultdict(list)
    for s in spendables:
        spendable_lookup[s.block_index_available].append(s)
        spendable_lookup[s.block_index_spent].append(s)
    balance = 0
    for bi in sorted(spendable_lookup.keys()):
        if bi == 0:
            continue
        sorted_spendables = sorted(spendable_lookup[bi], key=lambda _: _.tx_hash)
        delta = 0
        txs = []
        for s in sorted_spendables:
            if s.block_index_available == bi:
                note = "%s %s" % (
                    args.network.ui.address_for_script(s.script), b2h_rev(s.tx_hash))
                delta += s.coin_value
                tx = "%16s mBTC %s" % (satoshis_to_amount(s.coin_value), note)
                txs.append(tx)
        for s in sorted_spendables:
            if s.block_index_spent == bi:
                note = args.network.ui.address_for_script(s.script)
                delta -= s.coin_value
                tx = "%16s mBTC %s" % (satoshis_to_amount(-s.coin_value), note)
                txs.append(tx)
        amount = satoshis_to_amount(delta)
        balance += delta
        if len(txs) > 1:
            print("      T %16s mBTC" % amount)
            for _ in txs:
                print("            %s" % _)
        else:
            print("      T %s" % txs[0])
        print("%7d: %14s" % (bi, satoshi_to_mbtc(balance)))


def create_parser():
    parser = argparse.ArgumentParser(description="SPV wallet.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.', default="~/.pycoin/wallet/")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('-w', '--wallet_name', help='The name of the wallet.', default="default")
    subparsers = parser.add_subparsers(help="commands", dest='command')

    fetch_parser = subparsers.add_parser('fetch', help='Update to current blockchain view')
    fetch_parser.add_argument('-d', "--date", help="Skip ahead to this date.",
                              type=lambda x: time.strptime(x, '%Y-%m-%d'),
                              default=time.strptime('2008-01-01', '%Y-%m-%d'))

    fetch_parser.add_argument(
        "peer", metavar="peer_ip[:port]", help="Fetch from this peer.", type=str, nargs="*")

    balance_parser = subparsers.add_parser('balance', help='Show wallet balance')
    balance_parser.add_argument("block", help="balance as of block", nargs="?", type=int)

    create_parser = subparsers.add_parser('tx', help='Create transaction')
    create_parser.add_argument("-o", "--output", type=str, help="name of tx output file", required=True)
    create_parser.add_argument("-F", "--fee", type=int, help="fee in satoshis", default=1000)
    create_parser.add_argument('payable', nargs='+',
                               help="payable: either a bitcoin address, or an address/amount combo")

    exclude_parser = subparsers.add_parser('exclude', help="Exclude spendables from a given transaction")
    exclude_parser.add_argument('path_to_tx', help="path to transaction")

    rewind_parser = subparsers.add_parser('rewind', help="Rewind to a given block")
    rewind_parser.add_argument('block_number', type=int, help="block number to rewind to")

    subparsers.add_parser('dump', help="Dump spendables")
    subparsers.add_parser('history', help="Show history")

    return parser


def main():
    init_logging()
    parser = create_parser()

    args = parser.parse_args()

    loop = asyncio.get_event_loop()

    if args.command == "fetch":
        wallet_fetch(args)
    if args.command == "balance":
        wallet_balance(args)
    if args.command == "tx":
        wallet_tx(args)
    if args.command == "exclude":
        wallet_exclude(args)
    if args.command == "rewind":
        wallet_rewind(args)
    if args.command == "dump":
        wallet_dump(args)
    if args.command == "history":
        wallet_history(args)


if __name__ == '__main__':
    main()
