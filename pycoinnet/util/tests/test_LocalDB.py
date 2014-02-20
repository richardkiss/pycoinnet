import hashlib

from tempfile import TemporaryDirectory

from pycoinnet.util.LocalDB import LocalDB
from pycoinnet.util.LocalDB_RAM import LocalDB as LocalDB_RAM

def stream_f(f, item):
    f.write(("%s\n" % item).encode("utf8"))

def parse_f(f):
    v = int(f.readline())
    return v

def hash_f(v):
    b = ("%d" % v).encode("utf8")
    return hashlib.sha256(b).digest()

def do_test_local_db(local_db):
    assert list(local_db.all_hashes()) == []
    local_db.add_items([500])
    assert list(local_db.all_hashes()) == [hash_f(500)]
    local_db.add_items([700])
    assert set(local_db.all_hashes()) == {hash_f(v) for v in [500, 700]}
    local_db.add_items([900])
    assert set(local_db.all_hashes()) == {hash_f(v) for v in [500, 700, 900]}
    local_db.remove_items_with_hash([hash_f(600)])
    assert set(local_db.all_hashes()) == {hash_f(v) for v in [500, 700, 900]}
    local_db.remove_items_with_hash([hash_f(700)])
    assert set(local_db.all_hashes()) == {hash_f(v) for v in [500, 900]}

    local_db.add_items(range(400,450))
    assert set(local_db.all_hashes()) == {hash_f(v) for v in [500, 900] + list(range(400,450))}
    for h in local_db.all_hashes():
        assert local_db.hash_is_known(h)
        item = local_db.item_for_hash(h)
        assert hash_f(item) == h

def test_localdb_1():
    with TemporaryDirectory() as the_dir:
        local_db = LocalDB(hash_f, stream_f, parse_f, the_dir)
        do_test_local_db(local_db)

def test_localdb_ram():
    local_db = LocalDB_RAM(hash_f)
    do_test_local_db(local_db)
