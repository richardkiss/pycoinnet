import logging
import os

from pycoin.serialize import b2h_rev

from .PetrifyDB_RAM import PetrifyDB_RAM

class PetrifyDB(PetrifyDB_RAM):
    PETRIFIED_FN = "petrified.bin"

    def __init__(self, dir_path, *args, **kwargs):
        self.dir_path = dir_path
        super(PetrifyDB, self).__init__(*args, **kwargs)

    def _load_petrified_hashes(self):
        def the_hashes(f):
            try:
                while 1:
                    d = f.read(16384)
                    if len(d) == 0:
                        return
                    while len(d) >= 32:
                        yield d[:32]
                        d = d[32:]
            except Exception:
                pass
        try:
            with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "rb") as f:
                return list(the_hashes(f))
        except Exception:
            return []

    def _petrify_hashes(self, items):
        super(PetrifyDB, self)._petrify_hashes(items)
        with open(os.path.join(self.dir_path, self.PETRIFIED_FN), "ab") as f:
            for item in items:
                h1 = item.hash()
                if len(h1) == 32:
                    f.write(h1)
            logging.debug("wrote %d items to petrified DB", len(items))
