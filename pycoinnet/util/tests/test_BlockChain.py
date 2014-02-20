
from pycoinnet.util.BlockChain import BlockChain
from pycoinnet.util.PetrifyDB_RAM import PetrifyDB_RAM
from pycoinnet.util.LocalDB_RAM import LocalDB


class BHO(object):
    def __init__(self, h, previous_block_hash=None, difficulty=10):
        self.h = h
        self.previous_block_hash = h-1 if previous_block_hash is None else previous_block_hash
        self.difficulty = difficulty

    def hash(self):
        return self.h

    def __repr__(self):
        return "<BHO: id:%s parent:%s difficulty:%s>" % (self.h, self.previous_block_hash, self.difficulty)


def test_basic():
    parent_for_0 = "motherless"
    petrify_db = PetrifyDB_RAM(parent_for_0)
    local_db = LocalDB()
    BC = BlockChain(local_db, petrify_db)
    ITEMS = [BHO(i) for i in range(100)]
    ITEMS[0].previous_block_hash = parent_for_0

    assert BC.longest_local_block_chain() == []
    assert BC.longest_local_block_chain_length() == 0
    assert set(BC.missing_parents()) == set()
    assert BC.petrified_block_count() == 0
    assert BC.last_petrified_hash() == parent_for_0
    assert not BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    assert BC.item_for_hash(0) is None
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[:5])
    assert ops == [("add", i, i) for i in range(5)]
    assert BC.petrified_block_count() == 0
    assert BC.last_petrified_hash() == parent_for_0
    assert BC.longest_local_block_chain() == [4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 5
    assert set(BC.missing_parents()) == {parent_for_0}
    assert BC.block_chain_size() == 5
    assert BC.petrified_block_count() == 0
    assert not BC.hash_is_known(-1)
    for i in range(5):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(6)
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[:7])
    assert ops == [("add", i, i) for i in range(5,7)]
    assert BC.petrified_block_count() == 0
    assert BC.last_petrified_hash() == parent_for_0
    assert BC.longest_local_block_chain() == [6, 5, 4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 7
    assert set(BC.missing_parents()) == {parent_for_0}
    assert BC.block_chain_size() == 7
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    for i in range(7):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(7)
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[10:14])
    assert ops == []
    assert BC.petrified_block_count() == 0
    assert BC.last_petrified_hash() == parent_for_0
    assert BC.longest_local_block_chain() == [6, 5, 4, 3, 2, 1, 0]
    assert BC.longest_local_block_chain_length() == 7
    assert set(BC.missing_parents()) == {parent_for_0, 9}
    assert BC.block_chain_size() == 7
    assert BC.petrified_block_count() == 0
    assert BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    for i in range(7):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(7)
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[7:10])
    assert ops == [("add", i, i) for i in range(7,14)]
    assert BC.longest_local_block_chain() == [13, 12, 11, 10, 9, 8, 7, 6, 5, 4]
    assert BC.longest_local_block_chain_length() == 10
    assert set(BC.missing_parents()) == {3}
    assert BC.petrified_block_count() == 4
    assert BC.last_petrified_hash() == 3
    assert BC.block_chain_size() == 14
    assert BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    for i in range(14):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(14)
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[90:])
    assert ops == []
    assert BC.longest_local_block_chain() == [13, 12, 11, 10, 9, 8, 7, 6, 5, 4]
    assert BC.longest_local_block_chain_length() == 10
    assert set(BC.missing_parents()) == {3, 89}
    assert BC.petrified_block_count() == 4
    assert BC.last_petrified_hash() == 3
    assert BC.block_chain_size() == 14
    assert BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    for i in range(14):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(14)
    assert BC.item_for_hash(-1) is None

    ops = BC.add_items(ITEMS[14:90])
    assert ops == [("add", i, i) for i in range(14,100)]
    assert BC.longest_local_block_chain() == [99, 98, 97, 96, 95, 94, 93, 92, 91, 90]
    assert BC.longest_local_block_chain_length() == 10
    assert set(BC.missing_parents()) == {89}
    assert BC.petrified_block_count() == 90
    assert BC.last_petrified_hash() == 89
    assert BC.block_chain_size() == 100
    assert BC.hash_is_known(0)
    assert not BC.hash_is_known(-1)
    for i in range(100):
        assert BC.hash_is_known(i)
        v = BC.item_for_hash(i)
        assert v.hash() == i
        assert v.previous_block_hash == parent_for_0 if i == 0 else i
    assert not BC.hash_is_known(100)
    assert BC.item_for_hash(-1) is None


def test_fork():
    parent_for_0 = b'\0' * 32
    # 0 <= 1 <= ... <= 5 <= 6
    # 3 <= 301 <= 302 <= 303 <= 304 <= 305

    petrify_db = PetrifyDB_RAM(parent_for_0)
    local_db = LocalDB()
    BC = BlockChain(local_db, petrify_db)
    ITEMS = dict((i, BHO(i)) for i in range(7))
    ITEMS[0].previous_block_hash = parent_for_0
    ITEMS.update((i, BHO(i)) for i in range(301, 306))
    ITEMS[301].previous_block_hash = 3

    assert BC.longest_local_block_chain() == []
    assert BC.longest_local_block_chain_length() == 0
    assert set(BC.missing_parents()) == set()

    # send them all except 302
    ops = BC.add_items((ITEMS[i] for i in ITEMS.keys() if i != 302))
    assert ops == [("add", i, i) for i in range(7)]
    assert set(BC.missing_parents()) == set([parent_for_0, 302])

    # now send 302
    ops = BC.add_items([ITEMS[302]])

    # we should see a change
    expected = [("remove", i, i) for i in range(6, 3, -1)]
    expected += [("add", i, i+4-301) for i in range(301,306)]
    assert ops == expected
    assert set(BC.missing_parents()) == set([parent_for_0])
