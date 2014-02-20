import hashlib

from tempfile import TemporaryDirectory

from pycoinnet.util.PetrifyDB import PetrifyDB
from pycoinnet.util.PetrifyDB_RAM import PetrifyDB_RAM, BlockHashOnly

def h_f(v):
    b = ("%d" % v).encode("utf8")
    return hashlib.sha256(b).digest()

BHOS = [BlockHashOnly(h_f(i), h_f(i-1), 10) for i in range(10000)]

def check_prepopulate(db):
    assert db.count_of_hashes() == 0
    assert db.last_hash() == BHOS[0].previous_block_hash
    db.add_chain(BHOS)

def check_postpopulate(db):
    assert db.count_of_hashes() == len(BHOS)
    for idx, bho in enumerate(BHOS):
        assert db.index_for_hash(bho.h) == idx
        assert db.hash_for_index(idx) == bho.h
        assert db.item_for_hash(bho.h).h == bho.h
        assert db.item_for_index(idx).h == bho.h
        assert db.hash_is_known(bho.h)
    assert db.all_hashes() == [bho.h for bho in BHOS]
    assert db.last_hash() == BHOS[-1].h

def test_PetrifyDB_RAM():
    db = PetrifyDB_RAM(parent_to_0=h_f(-1))
    check_prepopulate(db)
    check_postpopulate(db)

def test_PetrifyDB():
    with TemporaryDirectory() as the_dir:
        db = PetrifyDB(dir_path=the_dir, parent_to_0=h_f(-1))
        check_prepopulate(db)
        check_postpopulate(db)

        db1 = PetrifyDB(dir_path=the_dir, parent_to_0=h_f(-1))
        check_postpopulate(db1)

        BHOS1 = [BlockHashOnly(h_f(i), h_f(i-1), 10) for i in range(10000,10050)]

        db2 = PetrifyDB(dir_path=the_dir, parent_to_0=h_f(-1))
        check_postpopulate(db2)
        db2.add_chain(BHOS1)

        db3 = PetrifyDB(dir_path=the_dir, parent_to_0=h_f(-1))
        assert db3.last_hash() == BHOS1[-1].h
        assert db3.all_hashes() == [bho.h for bho in BHOS + BHOS1]
