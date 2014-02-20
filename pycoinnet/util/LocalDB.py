import binascii
import logging
import re
import os

from pycoin.block import BlockHeader
from pycoin.serialize import b2h_rev

from .LocalDB_RAM import LocalDB as LocalDB_RAM


def h2b_rev(h):
    return bytes(reversed(binascii.unhexlify(h)))


class LocalDB(LocalDB_RAM):
    def __init__(self, hash_f=lambda b: b.hash(), stream_f=lambda b: b.stream, parse_f=BlockHeader.parse, dir_path=None):
        super(LocalDB, self).__init__(hash_f)
        self.stream_f = stream_f
        self.parse_f = parse_f
        self.dir_path = dir_path

    def all_hashes(self):
        paths = os.listdir(self.dir_path)
        paths.sort()
        for p in paths:
            if re.match(r"[0-9a-f]{64}", p):
                yield h2b_rev(p)

    def _store_item(self, item):
        the_id = b2h_rev(self.hash_f(item))
        with open(os.path.join(self.dir_path, the_id), "wb") as f:
            self.stream_f(f, item)

    def _remove_item_with_hash(self, h):
        path = os.path.join(self.dir_path, b2h_rev(h))
        try:
            os.remove(path)
        except FileNotFoundError:
            logging.info("missing %s already", path)

    def _load_item_for_hash(self, h):
        p = b2h_rev(h)
        try:
            path = os.path.join(self.dir_path, p)
            with open(path, "rb") as f:
                item = self.parse_f(f)
                the_hash = self.hash_f(item)
                if b2h_rev(the_hash) == p:
                    return item
        except Exception:
            pass

