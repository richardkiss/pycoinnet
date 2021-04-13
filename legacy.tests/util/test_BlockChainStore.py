import hashlib

from tempfile import TemporaryDirectory

from pycoinnet.util.BlockChainStore import BlockChainStore, FakeHeader

def h_f(v):
    b = ("%d" % v).encode("utf8")
    return hashlib.sha256(b).digest()

BHOS = [FakeHeader(h_f(i), h_f(i-1)) for i in range(10000)]

def check_prepopulate(db):
    assert list(db.headers()) == []
    db.did_lock_to_index(BHOS, 0)

def check_postpopulate(db):
    assert list(db.headers()) == BHOS

def test_BlockChainStore():
    with TemporaryDirectory() as the_dir:
        db = BlockChainStore(dir_path=the_dir, parent_to_0=h_f(-1))
        check_prepopulate(db)
        check_postpopulate(db)

        db1 = BlockChainStore(dir_path=the_dir, parent_to_0=h_f(-1))
        check_postpopulate(db1)

        BHOS1 = [FakeHeader(h_f(i), h_f(i-1)) for i in range(10000,10050)]

        db2 = BlockChainStore(dir_path=the_dir, parent_to_0=h_f(-1))
        check_postpopulate(db2)
        db2.did_lock_to_index(BHOS1, len(BHOS))
        assert list(db2.headers()) == BHOS + BHOS1

        db3 = BlockChainStore(dir_path=the_dir, parent_to_0=h_f(-1))
        assert list(db3.headers()) == BHOS + BHOS1
