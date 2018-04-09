#!/usr/bin/env python

import argparse
import asyncio
import calendar
import codecs
import collections
import datetime
import logging
import io
import os.path
import sqlite3
import time

from pycoin.bloomfilter import BloomFilter, filter_size_required, hash_function_count_required
from pycoin.convention import satoshi_to_mbtc
from pycoin.key.validate import is_address_valid
from pycoin.message.InvItem import ITEM_TYPE_MERKLEBLOCK, InvItem
from pycoin.tx import Tx
from pycoin.tx.tx_utils import create_tx
from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
from pycoin.wallet.SQLite3Wallet import SQLite3Wallet

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.blockcatchup import create_peer_to_block_pipe, improve_headers
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.networks import MAINNET
from pycoinnet.pong_manager import install_pong_manager

from .common import init_logging, peer_connect_pipeline


def storage_base_path():
    p = os.path.expanduser("~/.pycoin/wallet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


class Keychain(object):
    def __init__(self, addresses):
        self.interested_addresses = set(addresses)

    def is_spendable_interesting(self, spendable):
        return spendable.bitcoin_address() in self.interested_addresses

    def addresses(self):
        return self.interested_addresses


def bloom_filter_from_parameters(element_count, false_positive_probability, tweak=1):
    filter_size = filter_size_required(element_count, false_positive_probability)
    hash_function_count = hash_function_count_required(filter_size, element_count)
    bloom_filter = BloomFilter(filter_size, hash_function_count=hash_function_count, tweak=1)
    print("%d elements; filter size: %d bytes; %d hash functions" % (
            element_count, filter_size, hash_function_count))
    return bloom_filter


def bloom_filter_for_addresses_spendables(addresses, spendables, false_positive_probability=0.0001):
    element_count = len(addresses) + len(spendables)
    bloom_filter = bloom_filter_from_parameters(element_count, false_positive_probability)
    for a in addresses:
        bloom_filter.add_address(a)
    for s in spendables:
        bloom_filter.add_spendable(s)
    return bloom_filter


def wallet_persistence_for_args(args):
    sql_db = sqlite3.Connection(os.path.join(args.path, "wallet.db"))
    persistence = SQLite3Persistence(sql_db)

    addresses = [a[:-1] for a in open(os.path.join(args.path, "watch_addresses")).readlines()]
    keychain = Keychain(addresses)

    wallet = SQLite3Wallet(keychain, persistence)
    bcv_json = persistence.get_global("blockchain_view") or "[]"
    bcv = BlockChainView.from_json(bcv_json)
    return wallet, persistence, bcv


async def get_peers(args):
    from pycoinnet.Peer import Peer
    from pycoinnet.version import version_data_for_peer
    network = args.network
    host, port = (args.peer_address, args.peer_port)
    reader, writer = await asyncio.open_connection(host=host, port=port)
    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    version_data = version_data_for_peer(peer)
    peer.version = await peer.perform_handshake(**version_data)
    return [peer]


async def wallet_fetch(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

    early_timestamp = calendar.timegm(args.date)

    print(args.path)
    print("wallet. Fetching.")

    # get archived headers

    if args.rewind:
        print("rewinding to block %d" % args.rewind)
        blockchain_view.rewind(args.rewind)

    spendables = list()  # persistence.unspent_spendables(blockchain_view.last_block_index()))

    bloom_filter = bloom_filter_for_addresses_spendables(wallet.keychain.addresses(), spendables)

    def filter_f(bh, pri):
        if bh.timestamp >= early_timestamp:
            return ITEM_TYPE_MERKLEBLOCK

    filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
    flags = 0  # BLOOM_UPDATE_ALL = 1  ## BRAIN DAMAGE

    #peer_to_block_pipe = create_peer_to_block_pipe(blockchain_view, filter_f)

    peers = await get_peers(args)
    for peer in peers:
        install_pong_manager(peer)
        peer.send_msg("filterload", filter=filter_bytes, tweak=tweak,
                      hash_function_count=hash_function_count, flags=flags)
        peer.start()

    # for now, let's just do one peer

    peer.set_request_callback("alert", lambda *args: None)

    inv_batcher = InvBatcher()
    await inv_batcher.add_peer(peer)

    while True:
        last_block_index = int(persistence.get_global("block_index") or 0)
        last_header_block_index = blockchain_view.last_block_index()
        if last_header_block_index < last_block_index + 2000:
            headers = await improve_headers(peer, blockchain_view, inv_batcher)
            block_number = blockchain_view.do_headers_improve_path(headers)
            bcv_json = blockchain_view.as_json()
            persistence.set_global("blockchain_view", bcv_json)
            continue
        last_block_index += 1
        (index, block_hash, total_work) = blockchain_view.tuple_for_index(last_block_index)
        f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_MERKLEBLOCK, block_hash))
        merkle_block = await f
        logging.debug("last_block_index = %s", last_block_index)
        if len(merkle_block.txs) > 0:
            logging.info(
                "got block %06d: %s... with %d transactions",
                index, merkle_block.id()[:32], len(merkle_block.txs))
        wallet._add_block(merkle_block, index, merkle_block.txs)
        if index % 1000 == 0:
            logging.info("at block %06d (%s)" % (
                index, datetime.datetime.fromtimestamp(merkle_block.timestamp)))
            persistence.commit()


def wallet_balance(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)
    last_block = persistence.get_global("last_block_index")
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


def wallet_create(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

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


def wallet_exclude(args):
    wallet, persistence, blockchain_view = wallet_persistence_for_args(args)

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


def create_parser():
    parser = argparse.ArgumentParser(description="SPV wallet.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.', default=storage_base_path())
    subparsers = parser.add_subparsers(help="commands", dest='command')

    fetch_parser = subparsers.add_parser('fetch', help='Update to current blockchain view')
    fetch_parser.add_argument('-d', "--date", help="Skip ahead to this date.",
                              type=lambda x: time.strptime(x, '%Y-%m-%d'),
                              default=time.strptime('2008-01-01', '%Y-%m-%d'))

    fetch_parser.add_argument('-r', "--rewind", help="Rewind to this block index.", type=int)
    fetch_parser.add_argument("peer_address", help="Fetch from this peer.", type=str)
    fetch_parser.add_argument("peer_port", help="Fetch from this peer port.", type=int, default=8333, nargs="?")

    subparsers.add_parser('balance', help='Show wallet balance')

    create_parser = subparsers.add_parser('create', help='Create transaction')
    create_parser.add_argument("-o", "--output", type=str, help="name of tx output file", required=True)
    create_parser.add_argument('payable', type=as_payable, nargs='+',
                               help="payable: either a bitcoin address, or a address/amount combo")

    exclude_parser = subparsers.add_parser('exclude', help="Exclude spendables from a given transaction")
    exclude_parser.add_argument('path_to_tx', help="path to transaction")
    return parser


def main():
    init_logging()
    parser = create_parser()

    args = parser.parse_args()

    args.network = MAINNET  # BRAIN DAMAGE

    loop = asyncio.get_event_loop()

    if args.command == "fetch":
        loop.run_until_complete(wallet_fetch(args))
    if args.command == "balance":
        wallet_balance(args)
    if args.command == "create":
        wallet_create(args)
    if args.command == "exclude":
        wallet_exclude(args.path, args)


if __name__ == '__main__':
    main()
