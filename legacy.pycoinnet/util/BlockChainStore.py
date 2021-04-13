import logging
import os


class FakeHeader:
    def __init__(self, h, previous_block_hash):
        self.h = h
        self.previous_block_hash = previous_block_hash
        self.difficulty = 1

    def hash(self):
        return self.h

    def __repr__(self):
        return "%s (parent %s)" % (self.h, self.previous_block_hash)

    def __eq__(self, other):
        return self.h == other.h and self.previous_block_hash == other.previous_block_hash

    def __hash__(self):
        return self.h.__hash__()


class BlockChainStore:
    BLOCK_HASHES_PATH = "locked_block_hashes.bin"

    def __init__(self, dir_path, parent_to_0=b'\0' * 32):
        self.dir_path = dir_path
        self.parent_to_0 = parent_to_0

    def block_tuple_iterator(self):
        try:
            with open(os.path.join(self.dir_path, self.BLOCK_HASHES_PATH), "rb") as f:
                prev_hash = self.parent_to_0
                while 1:
                    d = f.read(16384)
                    if len(d) == 0:
                        return
                    while len(d) >= 32:
                        the_hash = d[:32]
                        yield (the_hash, prev_hash, 1)
                        prev_hash = the_hash
                        d = d[32:]
        except Exception:
            pass

    def headers(self):
        for the_hash, prev_hash, weight in self.block_tuple_iterator():
            yield FakeHeader(the_hash, prev_hash)

    def did_lock_to_index(self, block_tuple_list, start_index):
        with open(os.path.join(self.dir_path, self.BLOCK_HASHES_PATH), "a+b") as f:
            pass
        with open(os.path.join(self.dir_path, self.BLOCK_HASHES_PATH), "r+b") as f:
            f.seek(start_index*32)
            count = 0
            # ## TODO: make sure the one we're writing is in the right place
            for the_hash, parent_hash, weight in block_tuple_list:
                f.write(the_hash)
                count += 1
            logging.debug("wrote %d items to block chain store at %s", count, self.dir_path)
