import logging
import os


class BlockChainStore:
    PETRIFIED_FN = "petrified.bin"

    def __init__(self, dir_path, parent_to_0=b'\0' * 32):
        self.dir_path = dir_path
        self.parent_to_0 = parent_to_0

    def block_tuple_iterator(self):
        try:
            with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "rb") as f:
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

    def did_lock_to_index(self, block_tuple_list, start_index):
        with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "a+b") as f:
            pass
        with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "r+b") as f:
            f.seek(start_index*32)
            count = 0
            ## TODO: make sure the one we're writing is in the right place
            for the_hash, parent_hash, difficulty in block_tuple_list:
                f.write(the_hash)
                count += 1
            logging.debug("wrote %d items to block chain store at %s", count, self.dir_path)
