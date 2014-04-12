import hashlib

from tempfile import TemporaryDirectory

from pycoinnet.util.DictStoreSimple import DictStoreSimple

def stream_f(f, item):
    f.write(("%s\n" % item).encode("utf8"))

def parse_f(f):
    v = int(f.readline())
    return v

def hash_f(v):
    b = ("%d" % v).encode("utf8")
    return hashlib.sha256(b).digest()

def add_items(db, items):
    for item in items:
        db[hash_f(item)] = item

def remove_items_with_hash(db, hashes):
    for h in hashes:
        if h in db:
            del db[h]

def do_test_local_db(local_db):
    assert list(local_db.keys()) == []
    add_items(local_db, [500])
    assert set(local_db.keys()) == {hash_f(500)}
    add_items(local_db, [700])
    assert set(local_db.keys()) == {hash_f(v) for v in [500, 700]}
    add_items(local_db, [900])
    assert set(local_db.keys()) == {hash_f(v) for v in [500, 700, 900]}
    remove_items_with_hash(local_db, [hash_f(600)])
    assert set(local_db.keys()) == {hash_f(v) for v in [500, 700, 900]}
    remove_items_with_hash(local_db, [hash_f(700)])
    assert set(local_db.keys()) == {hash_f(v) for v in [500, 900]}
    add_items(local_db, range(400,450))
    assert set(local_db.keys()) == {hash_f(v) for v in [500, 900] + list(range(400,450))}
    for h in local_db.keys():
        assert h in local_db
        item = local_db.get(h)
        assert hash_f(item) == h
        item = local_db[h]
        assert hash_f(item) == h

def test_localdb_1():
    with TemporaryDirectory() as the_dir:
        local_db = DictStoreSimple("t_", parse_f, stream_f, the_dir)
        do_test_local_db(local_db)
        local_db = DictStoreSimple("t_", parse_f, stream_f, the_dir)
        assert set(local_db.keys()) == {hash_f(v) for v in [500, 900] + list(range(400,450))}
        for h in local_db.keys():
            assert h in local_db
            item = local_db.get(h)
            assert hash_f(item) == h
            item = local_db[h]
            assert hash_f(item) == h

def test_localdb_ram():
    local_db = {}
    do_test_local_db(local_db)
