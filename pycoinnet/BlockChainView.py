import json

from pycoin.encoding.hexbytes import b2h_rev, h2b_rev


class BlockChainView:
    """
    A headers-view of the block chain. Keeps track of index, hash, work, with
    indices.
    """
    def __init__(self, node_tuples=[], hash_initial_block=b'\0' * 32):
        """
        A node_tuple is (index, hash, total_work).
        """
        self._set_tuples(node_tuples)
        self._genesis_tuple = (-1, hash_initial_block, 0)

    def _set_tuples(self, node_tuples):
        self.node_tuples = []
        self.hash_to_index = dict()
        self._add_tuples(node_tuples)

    def _add_tuples(self, node_tuples):
        nt = set(tuple(t) for t in node_tuples)
        self.node_tuples.extend(sorted(nt))

    def as_json(self, **kwargs):
        """
        Return as text, ready for storage. Not maximally efficient, but simple,
        and perfectly reasonable if only storing the winnowed view.
        """
        return json.dumps([[t[0], b2h_rev(t[1]), t[2]] for t in self.node_tuples], **kwargs)

    def hash_initial_block(self):
        return self._genesis_tuple[1]

    @classmethod
    def from_json(class_, the_json):
        def from_tuple(t):
            return [t[0], h2b_rev(t[1]), t[2]]
        return BlockChainView(node_tuples=[from_tuple(t) for t in json.loads(the_json)])

    def clone(self):
        return BlockChainView(node_tuples=list(self.node_tuples))

    def last_block_tuple(self):
        if len(self.node_tuples) == 0:
            return self._genesis_tuple
        return self.node_tuples[-1]

    def last_block_index(self):
        return self.last_block_tuple()[0]

    def tuple_for_index(self, index):
        """
        Return the node with the largest index <= the given index.
        In other words, this is the node we need to rewind to.
        """
        lo = 0
        hi = len(self.node_tuples)
        if hi == 0:
            return self._genesis_tuple
        while lo < hi:
            idx = int((lo+hi)/2)
            if self.node_tuples[idx][0] > index:
                hi = idx
            else:
                lo = idx + 1
        return self.node_tuples[hi-1]

    def tuple_for_hash(self, hash):
        if hash == self.hash_initial_block():
            return self._genesis_tuple
        idx = self.hash_to_index.get(hash)
        if idx is not None:
            return self.tuple_for_index(idx)
        for idx, h, tw in self.node_tuples[::-1]:
            self.hash_to_index[h] = idx
            if h == hash:
                return (idx, h, tw)
        return None

    def key_index_generator(self):
        index = self.last_block_index()
        step_size = 1
        count = 10
        while index > 0:
            yield index
            index -= step_size
            count -= 1
            if count <= 0:
                count = 10
                step_size *= 2
        yield 0

    def block_locator_hashes(self):
        """
        Generate locator_hashes value suitable for passing to getheaders message.
        """
        if len(self.node_tuples) == 0:
            return [self.hash_initial_block()]
        hashes = []
        for index in self.key_index_generator():
            the_hash = self.tuple_for_index(index)[1]
            if len(hashes) == 0 or the_hash != hashes[-1]:
                hashes.append(the_hash)
        return hashes

    @staticmethod
    def _halfsies_indices(block_index):
        s = set()
        step_size = 1
        count = 2
        while block_index >= 0:
            s.add(block_index)
            block_index -= step_size
            count -= 1
            if count <= 0 and block_index % (step_size*2) == 0:
                step_size *= 2
                count = 2
        return s

    def winnow(self, prior_to_block=None):
        """
        This method thins out the node_tuples using the "halfsies" method.
        """
        prior_to_block = prior_to_block or self.last_block_index()
        halfsies_indices = self._halfsies_indices(prior_to_block)
        old_node_tuples = self.node_tuples
        self._set_tuples(t for t in old_node_tuples if t[0] in halfsies_indices or t[0] > prior_to_block)

    def rewind(self, new_block_index):
        self._set_tuples(nt for nt in self.node_tuples if nt[0] <= new_block_index)
        return self

    def do_headers_improve_path(self, headers):
        """
        Returns False if the headers don't improve the path.

        If the headers DO improve the path, return the value of the block index of
        the first header.

        So you need to rewind to "new_start_idx" before applying the new blocks.
        """
        if len(headers) == 0:
            return False
        tuples = []
        if len(self.node_tuples) == 0:
            if headers[0].previous_block_hash != self.hash_initial_block():
                return False
            the_tuple = self._genesis_tuple
        else:
            the_tuple = self.tuple_for_hash(headers[0].previous_block_hash)
            if the_tuple is None:
                return False
        new_start_idx = the_tuple[0] + 1
        total_work = the_tuple[-1]
        expected_prior_hash = the_tuple[1]
        for idx, h in enumerate(headers):
            if h.previous_block_hash != expected_prior_hash:
                break
            total_work += 1  # TODO: make this difficulty/work instead of path size
            expected_prior_hash = h.hash()
            tuples.append((idx + new_start_idx, expected_prior_hash, total_work))
        if total_work <= self.last_block_tuple()[-1]:
            return False

        # the headers DO improve things
        self._add_tuples(tuples)
        return new_start_idx

    def __repr__(self):
        t = self.last_block_tuple()
        return "<BlockChainView tip: %d (%s...)>" % (t[0], b2h_rev(t[1])[:32])
