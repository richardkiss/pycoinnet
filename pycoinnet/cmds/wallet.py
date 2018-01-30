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

from pycoin.convention import satoshi_to_mbtc
from pycoin.key.validate import is_address_valid
from pycoin.serialize import b2h_rev
from pycoin.tx import Tx
from pycoin.tx.tx_utils import create_tx
from pycoin.wallet.SQLite3Persistence import SQLite3Persistence
# from pycoin.wallet.SQLite3Wallet import SQLite3Wallet

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_lists
from pycoinnet.networks import MAINNET
from pycoinnet.InvFetcher import InvFetcher
from pycoinnet.Peer import Peer

from pycoinnet.BloomFilter import BloomFilter, filter_size_required, hash_function_count_required
from pycoinnet.BlockChainView import BlockChainView, HASH_INITIAL_BLOCK
from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_MERKLEBLOCK
from pycoinnet.msg.PeerAddress import PeerAddress


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


@asyncio.coroutine
def _fetch_missing(peer, header):
    the_hash = header.previous_block_hash
    inv_item = InvItem(ITEM_TYPE_MERKLEBLOCK, the_hash)
    logging.info("requesting missing block header %s", inv_item)
    peer.send_msg("getdata", items=[inv_item])
    name, data = yield from peer.wait_for_response('merkleblock')
    block = data["header"]
    logging.info("got missing block %s", block.id())
    return block


@asyncio.coroutine
def do_get_headers(peer, block_locator_hashes, hash_stop=b'\0'*32):
    peer.send_msg(message_name="getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
    name, data = yield from peer.wait_for_response('headers')
    headers = [bh for bh, t in data["headers"]]
    return headers


@asyncio.coroutine
def do_updates(bcv, network, nonce, last_block_index, bloom_filter, early_timestamp, host, port):
    VERSION_MSG = dict(
            version=70001, subversion=b"/Notoshi/", services=1, timestamp=int(time.time()),
            remote_address=PeerAddress(1, host, port),
            local_address=PeerAddress(1, "127.0.0.1", 6111),
            nonce=nonce,
            last_block_index=last_block_index
        )
    
    reader, writer = yield from asyncio.open_connection(host=host, port=port)
    logging.info("connecting to %s:%d", host, port)
    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    remote_handshake = yield from peer.perform_handshake(**VERSION_MSG)
    assert remote_handshake is not None
    peer.start_dispatcher()

    filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
    # TODO: figure out flags
    flags = 0
    peer.send_msg(
        "filterload", filter=filter_bytes, hash_function_count=hash_function_count,
        tweak=tweak, flags=flags)

    inv_fetcher = InvFetcher(peer)
    msg_handler_id = peer.add_msg_handler(inv_fetcher.handle_msg)
    assert msg_handler_id is not None
    while True:
        block_locator_hashes = bcv.block_locator_hashes()
        headers = yield from do_get_headers(peer, bcv.block_locator_hashes())
        if block_locator_hashes[-1] == HASH_INITIAL_BLOCK:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            extra_block = yield from _fetch_missing(peer, headers[0])
            headers = [extra_block] + headers

        if len(headers) == 0:
            break

        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            break
        logging.debug("block header count is now %d", block_number)
        block_index = block_number
        index, block_hash, work = bcv.tuple_for_index(block_index)
        # find the header with this hash

        for idx in range(len(headers)):
            if headers[idx].hash() == block_hash:
                break
        else:
            logging.error("can't find block header with hash %s", b2h_rev(block_hash))
            break

        block_futures = []
        start_index = block_index + idx
        for offset, header in enumerate(headers):
            index = start_index + offset
            if index % 1000 == 0:
                print("at header %6d (%s)" % (
                    index, datetime.datetime.fromtimestamp(header.timestamp)))
            if header.timestamp < early_timestamp:
                continue
            inv_item = InvItem(item_type=ITEM_TYPE_MERKLEBLOCK, data=header.hash())
            block_futures.append((index, inv_fetcher.fetch(inv_item)))
            block_index += 1

        def process_tx(tx):
            print("processing %s" % tx)

        if len(block_futures) > 0:
            import pdb; pdb.set_trace()
        for index, bf in block_futures:
            merkle_block = yield from bf
            if len(merkle_block.tx_futures) > 0:
                print("got block %6d: %s... with %d transactions" % (
                    index, merkle_block.id()[:32], len(merkle_block.tx_futures)))
            for tx_f in merkle_block.tx_futures:
                tx = yield from tx_f
                process_tx(tx)
        bcv.winnow()


def wallet_fetch(path, args):
    early_timestamp = calendar.timegm(args.date)

    print(path)
    print("wallet. Fetching.")
    network = MAINNET

    addresses = [a[:-1] for a in open(os.path.join(path, "watch_addresses")).readlines()]
    keychain = Keychain(addresses)

    # get archived headers

    archived_headers_path = os.path.join(path, "archived_headers")
    try:
        with open(archived_headers_path) as f:
            bcv_json = f.read()
        blockchain_view = BlockChainView.from_json(bcv_json)
    except Exception:
        logging.exception("can't parse %s", archived_headers_path)
        blockchain_view = BlockChainView()

    if args.rewind:
        print("rewinding to block %d" % args.rewind)
        blockchain_view.rewind(args.rewind)

    spendables = list()  # persistence.unspent_spendables(blockchain_view.last_block_index()))

    element_count = len(addresses) + len(spendables)
    false_positive_probability = 0.0000001

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

    # next: connect to a host

    host_futures = dns_bootstrap_host_port_lists(network, 10)
    
    loop = asyncio.get_event_loop()
    host, port = loop.run_until_complete(host_futures[0])

    USE_LOCAL_HOST = True  # False
    if USE_LOCAL_HOST:
        # use a local host instead of going to DNS
        host = "127.0.0.1"
        port = 38333

    nonce = 3412075413544046060
    last_block_index = 1
    loop.run_until_complete(do_updates(
        blockchain_view, network, nonce, last_block_index, bloom_filter, early_timestamp, host, port))

    return
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
