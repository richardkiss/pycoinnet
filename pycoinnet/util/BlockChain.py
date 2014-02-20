import binascii
import logging
import os
import weakref

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev
from pycoinnet.util.ChainFinder import ChainFinder
from pycoinnet.util.Queue import Queue


"""
BlockChain

Usage:
- create it (with a local_db, a petrify_db, and a petrify_policy)
    - petrify_policy(petrified_chain_size, total_chain_size) => # of blocks to petrify

- add_items (either a Block or a BlockHeader)
  - returns added_chain, deleted_chain (if any)


- last_item_index
- hash_is_known (can just use item_for_hash)
- index_for_hash
- hash_for_index
- item_for_hash
- petrify_blocks(subchain)
  - adds to petrified_db
  - rebuilds ChainFinder object
  - remove obsolete blocks from local db
    - items moved to petrified_db
    - items pointing to now petrified chain
    - items very old
"""

def bh_to_node(bh):
    return bh.hash(), bh.previous_block_hash

def default_petrify_policy(unpetrified_chain_size, total_chain_size):
    """
    Default policy: keep all but the last ten blocks petrified.
    """
    return max(0, unpetrified_chain_size - 10)

def _update_q(q, ops):
    # first, we meld out complimentary adds and removes
    while len(ops) > 0:
        op = ops[0]
        if op[0] != 'remove':
            break
        last = q.pop()
        if op[1:] != last[1:]:
            q.put_nowait(last)
            break
        ops = ops[1:]
    for op in ops:
        q.put_nowait(op)


class BlockChain(object):
    """
    This is a compound object that accepts blocks (or block headers) via add_items.

    It also accepts a "petrify_policy", that is, a determination as to when blocks
    are deemed "too far" back in the chain to ever change, and should thus be archived
    in the petrify_db instead of hte local_db. This petrify_db can use a more efficient
    storage mechanism.

    There are two chains: the local chain, which is still open to be altered if a better
    subtree is found, and the petrified chain, which will never change because it's simply
    buried too deep.
    """

    def __init__(self, local_db, petrify_db, petrify_policy=default_petrify_policy):
        self.local_db = local_db
        self.petrify_db = petrify_db
        self.petrify_policy = petrify_policy
        self.excluded_hashes = set()
        self._create_chain_finder()
        self.change_queues = set() #weakref.WeakSet()

    def new_change_q(self):
        q = Queue()
        self.change_queues.add(q)
        return q

    def _create_chain_finder(self):
        self.chain_finder = ChainFinder()
        self.chain_finder.load_nodes(
            bh_to_node(self.local_db.item_for_hash(h)) for h in self.local_db.all_hashes()
                if not self.is_hash_excluded(h))
        self._longest_chain_cache = None

    def longest_local_block_chain(self):
        def node_weight_f(h):
            item = self.local_db.item_for_hash(h)
            if item:
                return item.difficulty
            return 0
        if self._longest_chain_cache is None:
            max_weight = 0
            longest = []
            for chain in self.chain_finder.all_chains_ending_at(self.petrify_db.last_hash()):
                weight = sum(node_weight_f(h) for h in chain)
                if weight > max_weight:
                    longest = chain
                    max_weight = weight
            self._longest_chain_cache = longest[:-1]
        return self._longest_chain_cache

    def longest_local_block_chain_length(self):
        return len(self.longest_local_block_chain())

    def missing_parents(self):
        return self.chain_finder.missing_parents()

    def is_hash_excluded(self, h):
        # an excluded hash is one that is either petrified (except the last one)
        # or one that has been proven to be invalid because it is bogus
        # or has an another excluded hash as parent
        if self.petrify_db.hash_is_known(h):
            return h != self.last_petrified_hash()
        # TODO: add and manage a local list of excluded hashes in some hash DB
        return h in self.excluded_hashes

    def exclude_hash(self, h):
        # add to list of excluded hashes
        self.excluded_hashes.add(h)

    def last_blockchain_hash(self):
        if self.longest_local_block_chain_length() > 0:
            return self.longest_local_block_chain()[0]
        return self.petrify_db.last_hash()

    def petrified_block_count(self):
        return self.petrify_db.count_of_hashes()

    def block_chain_size(self):
        return self.longest_local_block_chain_length() + self.petrified_block_count()

    def hash_is_known(self, h):
        return self.petrify_db.hash_is_known(h) or self.local_db.hash_is_known(h)

    def hash_for_index(self, idx):
        h = self.petrify_db.hash_for_index(idx)
        if h:
            return h
        idx -= self.petrified_block_count()
        lc = self.longest_local_block_chain()
        if 0 <= idx < len(lc):
            return lc[idx]

    def item_for_hash(self, h):
        v = self.local_db.item_for_hash(h)
        if v:
            return v
        return self.petrify_db.item_for_hash(h)

    def last_petrified_hash(self):
        return self.petrify_db.last_hash()

    def add_items(self, items):
        def items_to_add(items):
            for item in items:
                if self.hash_is_known(item.hash()):
                    continue
                self.local_db.add_items([item])
                yield bh_to_node(item)

        old_petrified_count = self.petrified_block_count()
        old_longest_chain = self.longest_local_block_chain()
        self.chain_finder.load_nodes(items_to_add(items))
        self._longest_chain_cache = None
        new_longest_chain = self.longest_local_block_chain()

        if old_longest_chain and new_longest_chain:
            old_path, new_path = self.chain_finder.find_ancestral_path(old_longest_chain[0], new_longest_chain[0])
            old_path = old_path[:-1]
            new_path = new_path[:-1]
        else:
            old_path = old_longest_chain
            new_path = new_longest_chain
        if old_path:
            logging.debug("old_path is %s-%s", old_path[0], old_path[-1])
        if new_path:
            logging.debug("new_path is %s-%s", new_path[0], new_path[-1])

        unpetrified_count = self.longest_local_block_chain_length()
        to_petrify_count = self.petrify_policy(
            unpetrified_count, unpetrified_count + old_petrified_count)

        if to_petrify_count > 0:
            self._petrify_blocks(to_petrify_count)

        # return a list of operations:
        # ("add"/"remove", the_hash, the_index)
        ops = []
        size = len(old_longest_chain) + old_petrified_count
        for idx, h in enumerate(old_path):
            op = ("remove", h, size-idx-1)
            ops.append(op)
        size = len(new_longest_chain) + old_petrified_count
        for idx, h in reversed(list(enumerate(new_path))):
            op = ("add", h, size-idx-1)
            ops.append(op)

        for q in self.change_queues:
            _update_q(q, ops)

        return ops

    def _petrify_blocks(self, to_petrify_count):
        petrify_list = self.longest_local_block_chain()[-to_petrify_count:]
        petrify_list.reverse()
        to_petrify_count = min(to_petrify_count, len(petrify_list))

        if to_petrify_count == 0:
            return
        items = [self.local_db.item_for_hash(h) for h in petrify_list]
        self.petrify_db.add_chain(items)
        self.petrify_db._log()

        self.local_db.remove_items_with_hash(petrify_list)

        self._create_chain_finder()

        # TODO: deal with orphan blocks!!
