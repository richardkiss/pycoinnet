import asyncio

from weakref import WeakSet

from pycoinnet.util.Multiplexer import Multiplexer


class obj:
    def f():
        pass


def test_WeakSet():
    # make sure WeakSet works as we expect
    s = WeakSet()
    # set should be empty
    assert len(s) == 0
    t = obj()
    s.add(t)
    # set should have t in it
    assert len(s) == 1
    t = None
    # make sure instance collected immediately
    assert len(s) == 0

    s = WeakSet()
    # set should be empty
    assert len(s) == 0

    t1 = obj()
    s.add(t1)
    # set should have t1 in it
    assert len(s) == 1
    f1 = t1.f
    # set should have t1 in it
    assert len(s) == 1
    t1 = None
    # set should have t1 in it as f1 refers to t1
    assert len(s) == 1
    f1 = None
    # no more references to t1, so set should be empty
    assert len(s) == 0

    t1 = obj()
    t2 = obj()
    s.add(t1)
    s.add(t2)
    # set should have t1 and t2
    assert len(s) == 2
    f1 = t1.f
    f2 = t2.f
    # set should have t1 and t2
    assert len(s) == 2
    t1 = None
    t2 = None
    # set should have t1 and t2
    assert len(s) == 2
    del f1
    # set should have just t2
    assert len(s) == 1
    del f2
    # set should be empty
    assert len(s) == 0


def test_creation():
    loop = asyncio.new_event_loop()

    v = 0

    @asyncio.coroutine
    def get_next():
        yield from asyncio.sleep(0.01, loop=loop)
        v += 1
        return v

    mp = Multiplexer(get_next, loop=loop)
    assert v == 0
    mp.cancel()
    loop.stop()
    loop.run_forever()
    loop.close()


def test_simple():
    v = 0

    loop = asyncio.new_event_loop()

    @asyncio.coroutine
    def get_next():
        yield from asyncio.sleep(0.01, loop=loop)
        nonlocal v
        v += 1
        return v

    mp = Multiplexer(get_next, loop=loop)
    # make sure get_next hasn't run yet
    assert v == 0

    loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(0.1, loop=loop), loop=loop))
    q1_get = mp.new_q()
    k1 = loop.run_until_complete(asyncio.ensure_future(q1_get(), loop=loop))

    # make sure that the first thing returned was 1
    assert k1 == 1

    # run_until_complete shouldn't let get_next run more than once
    assert v == 1

    r = loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(0.1, loop=loop), loop=loop))
    # check that sleep returned None
    assert r is None

    v1 = v
    q2_get = mp.new_q()
    k1 = loop.run_until_complete(asyncio.ensure_future(q1_get(), loop=loop))
    k2 = loop.run_until_complete(asyncio.ensure_future(q2_get(), loop=loop))

    # check that q1 is at 2
    assert k1 == 2

    # check that q2 is at v1 + 1
    assert k2 == v1 + 1

    k2 = loop.run_until_complete(asyncio.ensure_future(q2_get(), loop=loop))
    # check the next thing to q2
    assert k2 == v1 + 2

    # shut down
    mp.cancel()
    loop.stop()
    loop.run_forever()
    loop.close()


def test_wait_until_listeners():
    # make sure that we don't start feeding the queues until there IS one

    loop = asyncio.new_event_loop()

    v = 0

    @asyncio.coroutine
    def get_next():
        yield from asyncio.sleep(0.01, loop=loop)
        nonlocal v
        v += 1
        return v

    mp = Multiplexer(get_next, loop=loop)

    # wait a bit
    loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(0.1, loop=loop), loop=loop))

    # ensure queue hasn't been fed
    # we allow v == 1 because we could be stalling on the
    # "post to queues" step
    assert v in (0, 1)

    # now create a new queue
    q1_get = mp.new_q()

    # get an item from the q1
    k = loop.run_until_complete(asyncio.ensure_future(q1_get(), loop=loop))
    assert k == 1

    # remove q1 queue
    mp.remove_q(q1_get)
    del q1_get

    # make sure mp.queue_set is empty
    assert len(mp.queue_set) == 0

    # make sure v doesn't change while we wait a while (since there is no place to put the messages)
    v1 = v
    loop.run_until_complete(asyncio.ensure_future(asyncio.sleep(0.1, loop=loop), loop=loop))
    v2 = v
    assert v1 in (v2 - 1, v2)

    # now create a new queue
    q2_get = mp.new_q()

    k1 = loop.run_until_complete(asyncio.ensure_future(q2_get(), loop=loop))
    k2 = loop.run_until_complete(asyncio.ensure_future(q2_get(), loop=loop))
    assert k1 + 1 == k2
    k3 = loop.run_until_complete(asyncio.ensure_future(q2_get(), loop=loop))
    assert k2 + 1 == k3

    # shut down
    mp.cancel()
    loop.stop()
    loop.run_forever()
    loop.close()
