from pycoinnet.util.TwoLevelDict import TwoLevelDict

def test_simple():
    d = TwoLevelDict()
    d["foo"] = "bar"
    for i in range(100):
        d[i] = i * i
    for i in range(100):
        assert d[i] == i * i

    d.rotate()

    for i in range(100):
        d[-i] = i * i

    d.rotate()

    for i in range(100):
        assert d[-i] == i * i
        if i>0:
            assert i not in d

    d.rotate()

    # let's touch some of them
    for i in range(50):
        v = d[-i]
        assert v == i * i

    d.rotate()

    for i in range(50):
        assert -i in d

    for i in range(50, 100):
        assert -i not in d
