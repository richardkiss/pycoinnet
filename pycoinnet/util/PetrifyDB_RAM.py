import logging

from pycoin.serialize import b2h_rev


class BlockHashOnly(object):
    def __init__(self, h, previous_block_hash, difficulty):
        self.h = h
        self.previous_block_hash = previous_block_hash
        self.difficulty = difficulty

    def hash(self):
        return self.h


class PetrifyError(Exception):
    pass

ZERO_HASH = b'\0' * 32

class PetrifyDB_RAM(object):
    def __init__(self, parent_to_0=ZERO_HASH):
        self.parent_to_0 = parent_to_0
        self.petrified_hashes = self._load_petrified_hashes()
        self.petrified_hashes_lookup = dict((v,k) for k,v in enumerate(self.petrified_hashes))

    def _log(self):
        logging.debug("petrified chain is length %d", len(self.petrified_hashes))
        if len(self.petrified_hashes):
            logging.debug("petrified chain starts with %s", self.petrified_hashes[0])
            logging.debug("petrified chain ends with %s", self.petrified_hashes[-1])
        if len(self.petrified_hashes_lookup) < len(self.petrified_hashes):
            logging.error("warning: petrified_hashes_lookup has %d members", len(self.petrified_hashes_lookup))

    def index_for_hash(self, h):
        return self.petrified_hashes_lookup.get(h)

    def hash_for_index(self, idx):
        if 0 <= idx < len(self.petrified_hashes):
            return self.petrified_hashes[idx]

    def item_for_hash(self, h):
        idx = self.index_for_hash(h)
        if idx is not None:
            return self.item_for_index(idx)

    def item_for_index(self, idx):
        pbh = self.hash_for_index(idx-1) if idx > 0 else self.parent_to_0
        return BlockHashOnly(h=self.hash_for_index(idx), previous_block_hash=pbh, difficulty=0)

    def hash_is_known(self, h):
        return h in self.petrified_hashes_lookup

    def count_of_hashes(self):
        return len(self.petrified_hashes)

    def all_hashes(self):
        return self.petrified_hashes

    def last_hash(self):
        if self.petrified_hashes:
            return self.petrified_hashes[-1]
        return self.parent_to_0

    def add_chain(self, items):
        last_hash = self.last_hash()
        logging.debug("last_hash = %s" % last_hash)
        logging.debug("hash of next block is %s", items[0].hash())
        logging.debug("points to prev block %s", items[0].previous_block_hash)
        if items[0].previous_block_hash != last_hash:
            raise PetrifyError("blockchain does not extend petrified chain (expecting %s)" % last_hash)
        self._petrify_hashes(items)

    def _load_petrified_hashes(self):
        return []

    def _petrify_hashes(self, items):
        for item in items:
            h = item.hash()
            self.petrified_hashes.append(h)
            self.petrified_hashes_lookup[h] = len(self.petrified_hashes) - 1
