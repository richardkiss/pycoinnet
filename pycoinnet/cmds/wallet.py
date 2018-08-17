#!/usr/bin/env python

import argparse
import calendar
import codecs
import datetime
import logging
import io
import os.path
import sqlite3
import textwrap
import time

from collections import defaultdict

from pycoin.bloomfilter import BloomFilter, filter_size_required, hash_function_count_required
from pycoin.convention import satoshi_to_mbtc
from pycoin.encoding.b58 import a2b_hashed_base58
from pycoin.encoding.hash import hash160
from pycoin.encoding.hexbytes import b2h_rev
from pycoin.key.Keychain import Keychain
from pycoin.encoding.hexbytes import b2h, h2b, h2b_rev
from pycoin.ui.validate import is_address_valid
from pycoin.networks.registry import network_for_netcode
from pycoin.message.InvItem import ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK
from pycoin.coins.tx_utils import create_tx

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.blockcatchup import fetch_blocks_after
from pycoinnet.peer_pipeline import get_peer_pipeline
from pycoinnet.PeerManager import PeerManager

from .MultisigKey import parse_MultisigKey

from .common import init_logging


class DB:
    def __init__(self, path):
        self._path = path
        self._db = sqlite3.Connection(self._path)
        self._init_tables()

    def _exec_sql_list(self, sql_list, args_list=None):
        if args_list is None:
            args_list = [[]] * len(sql_list)
        c = self._db.cursor()
        for sql, args in zip(sql_list, args_list):
            sql = textwrap.dedent(sql)
            c.execute(sql, args)
        self._db.commit()
        return c

    def _exec_sql(self, sql, *args):
        return self._exec_sql_list([sql], [args])

    def commit(self):
        self._db.commit()


class GlobalDB(DB):
    def _init_tables(self):
        SQL = """
            create table if not exists Global (
                slug text primary key,
                data text
            );"""
        self._exec_sql(SQL)

    def set_global(self, slug, value):
        self._exec_sql("insert or replace into Global values (?, ?)", slug, value)

    def get_global(self, slug):
        c = self._exec_sql("select data from Global where slug = ?", slug)
        r = c.fetchone()
        if r is None:
            return r
        return r[0]


class SpendableDB(DB):
    def _init_tables(self):
        SQL = [
            """
                create table if not exists Spendable (
                    tx_hash text,
                    tx_out_index integer,
                    coin_value integer,
                    script text,
                    block_index_available integer,
                    does_seem_spent boolean,
                    block_index_spent integer,
                    unique(tx_hash, tx_out_index)
                );""",
            "create index if not exists Spendable_cv on Spendable (coin_value);",
            "create index if not exists Spendable_bia on Spendable (block_index_available);",
            "create index if not exists Spendable_bis on Spendable (block_index_spent);"]

        self._exec_sql_list(SQL)

    def rewind_spendables(self, block_index):
        SQL1 = ("update Spendable set block_index_available = 0 where block_index_available > ?")
        self._exec_sql(SQL1, block_index)

        SQL2 = ("update Spendable set block_index_spent = 0 where block_index_spent > ?")
        self._exec_sql(SQL2, block_index)

    def unspent_spendables(self, last_block, spendable_class, confirmations=0):
        # we fetch spendables "old enough"
        # we alternate between "biggest" and "smallest" spendables
        SQL = ("select tx_hash, tx_out_index, coin_value, script, block_index_available, "
               "does_seem_spent, block_index_spent from Spendable where "
               "block_index_available > 0 and does_seem_spent = 0 and block_index_spent = 0 "
               "%s order by coin_value %s")

        if confirmations > 0:
            prior_to_block = last_block + 1 - confirmations
            t1 = "and block_index_available <= %d " % prior_to_block
        else:
            t1 = ""

        c1 = self._exec_sql(SQL % (t1, "desc"))
        c2 = self._exec_sql(SQL % (t1, "asc"))

        seen = set()
        try:
            while 1:
                r = next(c2)
                s = self.spendable_for_row(r, spendable_class)
                name = (s.tx_hash, s.tx_out_index)
                if name not in seen:
                    yield s
                seen.add(name)
                r = next(c1)
                s = self.spendable_for_row(r, spendable_class)
                name = (s.tx_hash, s.tx_out_index)
                if name not in seen:
                    yield s
                seen.add(name)
        except StopIteration:
            pass

    def save_spendable(self, spendable):
        tx_hash = b2h_rev(spendable.tx_hash)
        script = b2h(spendable.script)
        self._exec_sql("insert or replace into Spendable values (?, ?, ?, ?, ?, ?, ?)", tx_hash,
                       spendable.tx_out_index, spendable.coin_value, script,
                       spendable.block_index_available, spendable.does_seem_spent,
                       spendable.block_index_spent)

    def spendable_for_hash_index(self, tx_hash, tx_out_index, spendable_class):
        tx_hash_hex = b2h_rev(tx_hash)
        SQL = ("select coin_value, script, block_index_available, "
               "does_seem_spent, block_index_spent from Spendable where "
               "tx_hash = ? and tx_out_index = ?")
        c = self._exec_sql(SQL, tx_hash_hex, tx_out_index)
        r = c.fetchone()
        if r is None:
            return r
        return spendable_class(coin_value=r[0], script=h2b(r[1]), tx_hash=tx_hash,
                               tx_out_index=tx_out_index, block_index_available=r[2],
                               does_seem_spent=r[3], block_index_spent=r[4])

    def all_spendables(self, spendable_class, qualifier_sql=""):
        SQL = ("select tx_hash, tx_out_index, coin_value, script, block_index_available, "
               "does_seem_spent, block_index_spent from Spendable " + qualifier_sql)
        c1 = self._exec_sql(SQL)
        try:
            while 1:
                r = next(c1)
                yield self.spendable_for_row(r, spendable_class)
        except StopIteration:
            pass

    @staticmethod
    def spendable_for_row(r, spendable_class):
        tx_hash_str = r[0]
        # BRAIN DAMAGE: fix this encoding stuff
        if isinstance(tx_hash_str, bytes):
            tx_hash_str = tx_hash_str.decode("utf8")
        tx_hash = h2b_rev(tx_hash_str)
        script_str = r[3]
        if isinstance(script_str, bytes):
            script_str = script_str.decode("utf8")
        script = h2b(script_str)
        return spendable_class(coin_value=r[2], script=script, tx_hash=tx_hash, tx_out_index=r[1],
                               block_index_available=r[4], does_seem_spent=r[5], block_index_spent=r[6])


class InterestFinder:
    def __init__(self, path, network, hash160_list, multisig_key):
        self._path = path
        self._network = network
        self._hash160_set = set(hash160_list)
        self._keychain = Keychain(sqlite3.connect(path))
        self._multisig_key = multisig_key

    def interesting_blobs_from_script(self, script):
        interesting = []
        for opcode, data, pc, new_pc in self._network.extras.ScriptTools.get_opcodes(script):
            if data and self.blob_is_interesting(script, data):
                interesting.append(data)
        return interesting

    def blob_is_interesting(self, script, data):
        if data in self._hash160_set:
            return True
        r = self._keychain.path_for_hash160(data)
        if r:
            return True
        r = self._keychain.p2s_for_hash(data)
        if r:
            return True
        return False

    def hash160_set(self):
        s = set(self._hash160_set)
        s.update(self._keychain.interested_hashes())
        return s

    def _note_interest_in_multisig(self, path):
        address, scripts, original_keys = self._multisig_key.address_preimages_interests()
        msk = self._multisig_key.subkey_for_path(path)
        address, scripts, keys = msk.address_preimages_interests()
        if scripts:
            # the first one is the relevant one
            self._keychain.add_p2s_script(scripts[0])
        self._keychain.add_keys_path(original_keys, path)

    def ensure_minimal_gap_limit(self):
        if self._multisig_key is None:
            return
        for _ in range(self._multisig_key.gap_limit):
            self._note_interest_in_multisig("0/%d" % _)
            self._note_interest_in_multisig("1/%d" % _)
        self._keychain.commit()

    def find_fingerprint_path_for_blob(self, blob):
        r = self._keychain.path_for_hash160(blob)
        if r:
            return r

        script = self._keychain.p2s_for_hash(blob)
        if script:
            for opcode, data, pc, new_pc in self._network.extras.ScriptTools.get_opcodes(script):
                # OMG this is disgusting
                h160 = hash160(data)
                r = self._keychain.path_for_hash160(h160)
                if r:
                    return r

    def handle_potential_gap_limit(self, blob):
        r = self.find_fingerprint_path_for_blob(blob)
        if not r:
            return
        fingerprint, path = r
        # now, what do we do with this?
        if "/" not in path:
            logging.error("path is not compound: %s", path)
            return
        rest, last = path.rsplit("/", 1)
        start = int(last) + 1
        for _ in range(start, start + self._multisig_key.gap_limit):
            self._note_interest_in_multisig("%s/%d" % (rest, _))
        self._keychain.commit()

    def address_for_index(self, index, is_change=False):
        msk = self._multisig_key.subkey_for_path("%d/%d" % (1 if is_change else 0, index))
        address, preimages, subkeys = msk.address_preimages_interests()
        return address

    def find_maximal_index(self, path_template):

        def _is_interested(index):
            path = path_template % index
            msk = self._multisig_key.subkey_for_path(path)
            address, preimages, subkeys = msk.address_preimages_interests()
            if preimages:
                # preimages[0] is the one that is used in the p2s
                return self._keychain.p2s_for_hash(hash160(preimages[0]))
            else:
                return self._keychain.path_for_hash160(subkeys[0].hash160())

        lower_bound, upper_bound = 0, 1
        while _is_interested(upper_bound):
            lower_bound = upper_bound
            upper_bound += upper_bound

        # binary search
        # we want the last one where _is_interested is true
        # we know _is_interested is true for lower_bound and false for upper_bound

        while lower_bound + 1 < upper_bound:
            trial = (lower_bound + upper_bound) // 2
            assert trial < upper_bound
            if _is_interested(trial):
                lower_bound = trial
            else:
                upper_bound = trial
        return lower_bound


class Wallet:
    def __init__(self, interest_finder, global_db, spendable_db):
        self._interest_finder = interest_finder
        self._global_db = global_db
        self._spendable_db = spendable_db

    def ensure_minimal_gap_limit(self):
        self._interest_finder.ensure_minimal_gap_limit()

    def hash160_set(self):
        return self._interest_finder.hash160_set()

    def last_block_index(self):
        v = self._global_db.get_global("block_index")
        if v is None:
            v = -1
        return int(v)

    def blockchain_view(self):
        bcv_json = self._global_db.get_global("blockchain_view") or "[]"
        return BlockChainView.from_json(bcv_json)

    def set_blockchain_view(self, blockchain_view):
        bcv_json = blockchain_view.as_json()
        self._global_db.set_global("blockchain_view", bcv_json)

    def unspent_spendables(self, *args, **kwargs):
        return self._spendable_db.unspent_spendables(*args, **kwargs)

    def all_spendables(self, *args, **kwargs):
        return self._spendable_db.all_spendables(*args, **kwargs)

    def spendable_for_hash_index(self, *args, **kwargs):
        return self._spendable_db.spendable_for_hash_index(*args, **kwargs)

    def save_spendable(self, *args, **kwargs):
        return self._spendable_db.save_spendable(*args, **kwargs)

    def set_last_block_index(self, index):
        self._global_db.set_global("block_index", index)

    def rewind(self, block_index):
        self.set_last_block_index(block_index-1)
        self._spendable_db.rewind_spendables(block_index)
        self._spendable_db.commit()

    def process_confirmed_spendables(self, new_spendables_and_blobs, block_index):
        for spendable, blobs in new_spendables_and_blobs:
            spendable.block_index_available = block_index
            self._spendable_db.save_spendable(spendable)

    def process_spent_spendables(self, spent_spendables, block_index):
        for spendable in spent_spendables:
            spendable.block_index_spent = block_index
            self._spendable_db.save_spendable(spendable)

    def filter_interesting_txs(self, tx_list):
        new_spendables_and_blobs = []
        spent_spendables = []
        for tx in tx_list:
            for tx_in in tx.txs_in:
                spendable = self._spendable_db.spendable_for_hash_index(
                    tx_in.previous_hash, tx_in.previous_index, tx.Spendable)
                if spendable:
                    spent_spendables.append(spendable)
            for spendable in tx.tx_outs_as_spendable():
                r = self._interest_finder.interesting_blobs_from_script(spendable.script)
                if r:
                    new_spendables_and_blobs.append([spendable, r])
        return new_spendables_and_blobs, spent_spendables

    def process_block(self, block, block_index, txs, blockchain_view):
        self.set_last_block_index(block_index)
        new_spendables_and_blobs, spent_spendables = self.filter_interesting_txs(txs)
        self.process_confirmed_spendables(new_spendables_and_blobs, block_index)
        self.process_spent_spendables(spent_spendables, block_index)

        # check if we need to add some new addresses
        for spendable, blobs in new_spendables_and_blobs:
            for blob in blobs:
                self._interest_finder.handle_potential_gap_limit(blob)

        blockchain_view.do_headers_improve_path([block])

    def address_for_index(self, *args, **kwargs):
        return self._interest_finder.address_for_index(*args, **kwargs)


class AddressUtils:
    def __init__(self, network, sqlite3_db, hash160_items=[]):
        self._hash160_set = set(hash160_items)
        self._network = network
        self._keychain = Keychain(sqlite3_db)

    def is_spendable_interesting(self, spendable):
        return len(self.interesting_blobs(spendable)) > 0

    def interesting_blobs(self, spendable):
        interesting = []
        for opcode, data, pc, new_pc in self._network.extras.ScriptTools.get_opcodes(spendable.script):
            if data in self._hash160_set:
                interesting.append(data)
                continue
            r = self._keychain.path_for_hash160(data)
            if r:
                interesting.append(data)
                continue
            r = self._keychain.p2s_for_hash(data)
            if r:
                interesting.append(data)
        return interesting


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


def wallet_for_args(args):
    basepath = os.path.join(os.path.expanduser(args.path), args.wallet_name, args.network.code)
    if not os.path.exists(basepath):
        os.makedirs(basepath)

    sql_path = os.path.join(basepath, "wallet.db")

    try:
        hash160_list = [a2b_hashed_base58(a[:-1])[1:] for a in open(
            os.path.join(basepath, "watch_addresses")).readlines()]
    except OSError:
        hash160_list = []

    keychain = AddressUtils(args.network, sqlite3.Connection(sql_path), hash160_list)
    lines = open(os.path.join(basepath, "multisig_key")).readlines()

    multisig_key = None
    try:
        multisig_key = parse_MultisigKey(lines[0].strip())
        multisig_key.gap_limit = 50
    except Exception:
        pass

    interest_finder = InterestFinder(sql_path, args.network, hash160_list, multisig_key)
    global_db = GlobalDB(sql_path)
    spendable_db = SpendableDB(sql_path)

    return Wallet(interest_finder, global_db, spendable_db)


def commit_to_persistence(wallet, blockchain_view, last_block=None):
    if last_block:
        blockchain_view.winnow(last_block)
    wallet.set_blockchain_view(blockchain_view)


def wallet_fetch(args):
    wallet = wallet_for_args(args)

    wallet.ensure_minimal_gap_limit()

    last_block_index = wallet.last_block_index()
    blockchain_view = wallet.blockchain_view()
    blockchain_view.rewind(last_block_index)
    wallet.rewind(last_block_index)

    spendables = list(wallet.unspent_spendables(last_block_index, args.network.tx.Spendable))

    bloom_filter = bloom_filter_for_addresses_spendables(
        wallet.hash160_set(), spendables, element_pad_count=2000)

    early_timestamp = calendar.timegm(args.date)

    peer_pipeline = get_peer_pipeline(args.network, args.peer)
    peer_manager = PeerManager(peer_pipeline, args.count)

    # BRAIN DAMAGE TODO: explicitly skip ahead past early_timestamp

    def filter_f(bh, pri):
        if bh.timestamp >= early_timestamp:
            return ITEM_TYPE_MERKLEBLOCK if args.spv else ITEM_TYPE_BLOCK

    filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
    flags = 1  # BLOOM_UPDATE_ALL = 1  # BRAIN DAMAGE

    async def new_peer_callback(peer):
        if args.spv:
            peer.send_msg("filterload", filter=filter_bytes, tweak=tweak,
                          hash_function_count=hash_function_count, flags=flags)

    index_hash_work_tuples = blockchain_view.node_tuples

    last_save_time = time.time()
    for block, last_block_index in fetch_blocks_after(
            args.network, index_hash_work_tuples, peer_pipeline=peer_manager.new_peer_pipeline(),
            filter_f=filter_f, new_peer_callback=new_peer_callback):
        logging.debug("last_block_index = %s (%s)", last_block_index,
                      datetime.datetime.fromtimestamp(block.timestamp))
        txs = block.txs
        if len(txs) > 0:
            logging.info(
                "got block %06d: %s... with %d transactions",
                last_block_index, block.id()[:32], len(txs))

        wallet.process_block(block, last_block_index, txs, blockchain_view)

        now = time.time()
        if now > last_save_time + 15:
            logging.info("checkpoint commit at block %06d (%s)" % (
                last_block_index, datetime.datetime.fromtimestamp(block.timestamp)))
            commit_to_persistence(wallet, blockchain_view, last_block_index)
            wallet.set_last_block_index(last_block_index)
            last_save_time = time.time()
    commit_to_persistence(wallet, blockchain_view, last_block_index)


def wallet_balance(args):
    wallet = wallet_for_args(args)
    last_block = args.block or wallet.last_block_index()
    total = 0
    for spendable in wallet.unspent_spendables(last_block, args.network.tx.Spendable, confirmations=1):
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
    wallet = wallet_for_args(args)

    fee = args.fee

    last_block = wallet.last_block_index()

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
    unspents = wallet.unspent_spendables(last_block, args.network.tx.Spendable, confirmations=1)

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
    wallet = wallet_for_args(args)

    with open(args.path_to_tx, "rb") as f:
        if f.name.endswith("hex"):
            f = io.BytesIO(codecs.getreader("hex_codec")(f).read())
        tx = args.network.tx.parse(f)

    for tx_in in tx.txs_in:
        spendable = wallet.spendable_for_hash_index(tx_in.previous_hash, tx_in.previous_index, tx.Spendable)
        if spendable:
            spendable.does_seem_spent = True
            wallet.save_spendable(spendable)


def wallet_rewind(args):
    wallet = wallet_for_args(args)
    last_block_index = min(wallet.last_block_index(), args.block_number)
    wallet.rewind(last_block_index)
    print("rewinding to block %d" % last_block_index)


def wallet_dump(args):
    wallet = wallet_for_args(args)
    lbi = wallet.last_block_index()

    for spendable in wallet.unspent_spendables(lbi, args.network.tx.Spendable, confirmations=1):
        if spendable.block_index_available != 0:
            print(spendable.as_text())


def satoshis_to_amount(s):
    if s >= 0:
        return "+%s " % satoshi_to_mbtc(s)
    return "(%s)" % satoshi_to_mbtc(s)


def wallet_history(args):
    wallet = wallet_for_args(args)

    spendable_lookup = defaultdict(list)
    for s in wallet.all_spendables(args.network.tx.Spendable):
        if s.block_index_available:
            spendable_lookup[s.block_index_available].append(s)
            if s.block_index_spent:
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


def wallet_address(args):
    wallet = wallet_for_args(args)
    template = "1/%d" if args.change else "0/%d"
    max_index = wallet._interest_finder.find_maximal_index(template)
    for _ in range(max_index+1):
        print("%s: %s" % (template % _, wallet.address_for_index(_)))


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
        "peer", metavar="peer_ip[/port]", help="Fetch from this peer.", type=str, nargs="*")
    fetch_parser.add_argument("-s", "--spv", help="Use SPV-style merkle blocks (CURRENTLY UNRELIABLE).", action="store_true")
    fetch_parser.add_argument("-c", "--count", help="Peer count", default=8, type=int)

    balance_parser = subparsers.add_parser('balance', help='Show wallet balance')
    balance_parser.add_argument("block", help="balance as of block", nargs="?", type=int)

    create_parser = subparsers.add_parser('tx', help='Create transaction')
    create_parser.add_argument("-o", "--output", metavar="OUTPUT_FILENAME", type=str, help="name of tx output file", required=True)
    create_parser.add_argument("-F", "--fee", type=int, help="fee in satoshis", default=1000)
    create_parser.add_argument('payable', nargs='+',
                               help="payable: either a bitcoin address, or an address/amount combo")

    exclude_parser = subparsers.add_parser('exclude', help="Exclude spendables from a given transaction")
    exclude_parser.add_argument('path_to_tx', help="path to transaction")

    rewind_parser = subparsers.add_parser('rewind', help="Rewind to a given block")
    rewind_parser.add_argument('block_number', type=int, help="block number to rewind to")

    subparsers.add_parser('dump', help="Dump spendables")
    subparsers.add_parser('history', help="Show history")

    address_parser = subparsers.add_parser('address', help="Dump addresses")
    address_parser.add_argument("-c", "--change", action="store_true", help="display change addresses")

    return parser


def main():
    init_logging()
    parser = create_parser()

    args = parser.parse_args()

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
    if args.command == "address":
        wallet_address(args)


if __name__ == '__main__':
    main()
