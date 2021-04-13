"""
This dictionary has two levels. Items are stored in the first level.

When .rotate is called, items in the first level get pushed into the second.

When an item is referenced, it's moved back into the first level.
"""

import collections.abc


class TwoLevelDict(collections.abc.MutableMapping):
    def __init__(self, *args, **kwargs):
        self.dict = dict(*args, **kwargs)
        self.old_dict = {}

    def __setitem__(self, key, v):
        return self.dict.__setitem__(key, v)

    def __getitem__(self, key):
        try:
            v = self.old_dict.__getitem__(key)
            self.dict[key] = v
            del self.old_dict[key]
        except KeyError:
            pass
        return self.dict.__getitem__(key)

    def __delitem__(self, key):
        for d in (self.dict, self.old_dict):
            if key in d:
                d.__delitem__(key)

    def __len__(self):
        return self.dict.__len__() + self.old_dict.__len__()

    def __iter__(self):
        for i in self.dict.__iter__():
            yield i
        for i in self.old_dict.__iter__():
            yield i

    def rotate(self):
        self.old_dict = self.dict
        self.dict = {}
