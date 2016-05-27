import logging

from pycoinnet.util.BlockChainView import BlockChainView
from tests.helper import make_blocks, make_headers

def test1():
    bcv = BlockChainView()
    assert bcv.last_block_index() == -1
    assert bcv.block_locator_hashes() == [b'\0' * 32]

    blocks = make_blocks(20)
    #bcv.got_headers(blocks)

    #assert bcv.last_block_index() == 20
    #assert bcv.block_locator_hashes() == [b'\0' * 32]


def test_halfsies():
    hi = BlockChainView._halsies_indices
    assert hi(0) == set([0])
    assert hi(1) == set([0, 1])
    assert hi(2) == set([0, 1, 2])
    assert hi(3) == set([0, 1, 2, 3])
    assert hi(4) == set([0, 2, 3, 4])
    assert hi(5) == set([0, 2, 3, 4, 5])
    assert hi(6) == set([0, 2, 4, 5, 6])
    assert hi(7) == set([0, 2, 4, 5, 6, 7])
    assert hi(8) == set([0, 2, 4, 6, 7, 8])
    assert hi(9) == set([0, 2, 4, 6, 7, 8, 9])
    assert hi(10) == set([0, 4, 6, 8, 9, 10])
    assert hi(14) == set([0, 4, 8, 10, 12, 13, 14])
    assert hi(16) == set([0, 4, 8, 10, 12, 14, 15, 16])
    assert hi(24) == set([0, 8, 12, 16, 18, 20, 22, 23, 24])
    assert hi(32) == set([0, 8, 16, 20, 24, 26, 28, 30, 31, 32])
    assert hi(33) == set([0, 8, 16, 20, 24, 26, 28, 30, 31, 32, 33])
    assert hi(34) == set([0, 8, 16, 20, 24, 28, 30, 32, 33, 34])
    assert hi(1000) == set(
        [0, 256, 384, 512, 640, 704, 768, 832, 864, 896, 928, 944,
         960, 968, 976, 984, 988, 992, 994, 996, 998, 999, 1000]
    )
    print(sorted(hi(150000)))
    assert hi(100000) == set(
        [0, 32768, 49152, 65536, 73728, 81920, 86016, 90112, 92160, 94208, 95232, 96256,
         97280, 97792, 98304, 98816, 99072, 99328, 99456, 99584, 99712, 99776, 99840,
         99872, 99904, 99936, 99952, 99968, 99976, 99984, 99988, 99992, 99994, 99996, 99998, 99999, 100000
        ])
    assert hi(150000) == set(
        [0, 32768, 65536, 81920, 98304, 114688, 122880, 131072, 135168, 139264, 141312, 143360,
        145408, 146432, 147456, 147968, 148480, 148736, 148992, 149248, 149376, 149504, 149632,
        149696, 149760, 149824, 149856, 149888, 149920, 149936, 149952, 149968, 149976, 149984,
        149988, 149992, 149994, 149996, 149998, 149999, 150000]
        )
    assert hi(200000) == set(
        [0, 65536, 98304, 131072, 147456, 163840, 172032, 180224, 184320, 188416, 190464, 192512,
         194560, 195584, 196608, 197632, 198144, 198656, 198912, 199168, 199424, 199552, 199680,
         199744, 199808, 199872, 199904, 199936, 199952, 199968, 199976, 199984, 199988, 199992,
         199994, 199996, 199998, 199999, 200000
        ])
    assert hi(1000000) == set(
        [0, 262144, 393216, 524288, 655360, 720896, 786432, 851968, 884736, 917504, 933888,
         950272, 966656, 974848, 983040, 987136, 991232, 993280, 995328, 996352, 997376, 997888,
         998400, 998912, 999168, 999424, 999552, 999680, 999744, 999808, 999872, 999904, 999936,
         999952, 999968, 999976, 999984, 999988, 999992, 999994, 999996, 999998, 999999, 1000000
        ])
    prior = hi(0)
    for i in range(500):
        v = hi(i)
        assert v.issubset(prior.union([i]))
        prior = v


def make_bcv(node_count):
    headers = make_headers(node_count)
    nodes = ((i, header.hash(), i) for i, header in enumerate(headers))
    return BlockChainView(nodes)


def test_winnow():
    hi = BlockChainView._halsies_indices
    for size in [100, 500, 720, 1000]:
        bcv = make_bcv(size)
        assert len(bcv.node_tuples) == size
        assert len(bcv.hash_to_index) == size
        bcv.winnow()
        sz = len(hi(size))
        assert len(bcv.node_tuples) == sz
        assert len(bcv.hash_to_index) == sz


def test_tuple_for_index():
    for size in [100, 500, 720, 1000]:
        bcv = make_bcv(size)
        for i in range(size):
            index, the_hash, the_work = bcv.tuple_for_index(i)
            assert index == i
        bcv.winnow()
        for i in range(size):
            index, the_hash, the_work = bcv.tuple_for_index(i)
            assert index <= i


def test_tuple_for_hash():
    for size in [100, 500, 720, 1000]:
        bcv = make_bcv(size)
        headers = make_headers(size)
        for idx, header in enumerate(headers):
            the_hash = header.hash()
            the_tuple = bcv.tuple_for_hash(the_hash)
            assert the_tuple[0] == idx
            assert the_tuple[1] == the_hash
        bcv.winnow()
        items = BlockChainView._halsies_indices(size-1)
        for idx, header in enumerate(headers):
            the_hash = header.hash()
            the_tuple = bcv.tuple_for_hash(the_hash)
            if idx in items:
                assert the_tuple[0] == idx
                assert the_tuple[1] == the_hash
            else:
                assert the_tuple == None


def test_block_locator_hashes():
    for size in [100, 500, 720, 1000]:
        headers = make_headers(size)
        nodes = [(i, headers[i].hash(), i) for i in range(size)]
        bcv = BlockChainView(nodes)
        blh = bcv.block_locator_hashes()
        tuples = [bcv.tuple_for_hash(h) for h in blh]
        indices = [t[0] for t in tuples]
        indices.sort()
        assert indices == sorted(bcv.key_index_generator())

        bcv.winnow()
        blh = bcv.block_locator_hashes()
        tuples = [bcv.tuple_for_hash(h) for h in blh]
        indices = [t[0] for t in tuples]
        indices.sort()
        assert indices == list(t[0] for t in bcv.node_tuples)


def test_improve_path():
    size = 100
    headers = make_headers(size)
    bcv = make_bcv(0)
    assert bcv.last_block_index() == -1
    for count in range(100):
        report = bcv.do_headers_improve_path([headers[count]])
        assert report == count
        assert bcv.last_block_index() == count
    # let's try to extend from 95
    headers = make_headers(10, headers[95])

    for count in range(96, 100):
        report = bcv.do_headers_improve_path(headers[0:count-95])
        assert report == False
    count = 100
    report = bcv.do_headers_improve_path(headers[0:count-95])
    assert report == 96

    report = bcv.do_headers_improve_path(headers[5:])
    assert report == 101


def test_json_encoding():
    bcv = BlockChainView()
    j = bcv.as_json()
    assert j == '[]'
    for size in (30, 100, 750):
        bcv = make_bcv(30)
        bcv.winnow()
        j = bcv.as_json()
        bcv1 = BlockChainView.from_json(j)
        assert bcv1.node_tuples == bcv.node_tuples
