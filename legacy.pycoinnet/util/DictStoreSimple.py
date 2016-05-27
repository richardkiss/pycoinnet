import binascii
import collections.abc
import os
import re

from pycoin.serialize import b2h_rev


class DictStoreSimple(collections.abc.MutableMapping):
    def __init__(self, prefix, parse_f, stream_f=lambda f, obj: obj.stream(f), dir_path=None):
        self.prefix = prefix
        self.parse_f = parse_f
        self.stream_f = stream_f
        self.dir_path = dir_path
        self.the_re = re.compile(r"%s[0-9a-f]{64}" % self.prefix)

    def _path_for_hash(self, key):
        return os.path.join(self.dir_path, "%s%s" % (self.prefix, b2h_rev(key)))

    def __setitem__(self, key, v):
        with open(self._path_for_hash(key), "wb") as f:
            self.stream_f(f, v)

    def __getitem__(self, key):
        try:
            path = self._path_for_hash(key)
            with open(path, "rb") as f:
                v = self.parse_f(f)
                return v
        except Exception:
            pass
        raise KeyError

    def __delitem__(self, key):
        try:
            path = self._path_for_hash(key)
            os.unlink(path)
            return
        except Exception:
            pass
        raise KeyError

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        paths = os.listdir(self.dir_path)
        paths.sort()
        for p in paths:
            if self.the_re.match(p):
                yield bytes(reversed(binascii.unhexlify(p[len(self.prefix):])))

    def keys(self):
        return self.__iter__()
