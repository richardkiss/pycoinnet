import asyncio
import time

from asyncio.unix_events import _UnixSelectorEventLoop


class TimelessEventLoop(_UnixSelectorEventLoop):
    def __init__(self, *args, **kwargs):
        super(TimelessEventLoop, self).__init__(*args, **kwargs)
        self._fake_time_offset = 0.0

    def time(self):
        return self._fake_time_offset + time.time()

    def _fast_forward_time(self):
        if not self._scheduled:
            return
        now = self.time()
        when = self._scheduled[0]._when
        if when > now:
            self._fake_time_offset += when - now
    
    def _run_once(self):
        """Run one full iteration of the event loop.

        This calls all currently ready callbacks, polls for I/O,
        schedules the resulting callbacks, and finally schedules
        'call_later' callbacks.

        It uses the "_fake_time" value.
        """
        self._fast_forward_time()
        super(TimelessEventLoop, self)._run_once()


TIMELESS_EVENT_LOOP = TimelessEventLoop()


def use_timeless_eventloop():
    asyncio.set_event_loop(TIMELESS_EVENT_LOOP)


class TimelessTransport(asyncio.Transport):
    def __init__(self, remote_protocol, latency=2.0):
        self._remote_protocol = remote_protocol
        self._loop = asyncio.get_event_loop()
        self._latency = latency
        self._is_closing = False

    def write(self, data):
        if self._is_closing:
            raise RuntimeError("can't write to closed transport")
        self._loop.call_later(self._latency, self._remote_protocol.data_received, data)

    def close(self):
        def later():
            self._remote_protocol.eof_received()
            self._remote_protocol.connection_lost(None)
        self._loop.call_later(self._latency, later)
        self._is_closing = True


def create_timeless_transport_pair(pf1, pf2=None):
    if pf2 is None:
        pf2 = pf1
    p1 = pf1()
    p2 = pf2()
    t1 = TimelessTransport(p2)
    t2 = TimelessTransport(p1)
    p1.connection_made(t1)
    p2.connection_made(t2)
    return (t1, p1), (t2, p2)


def create_timeless_streams_pair():
    loop = asyncio.get_event_loop()
    r1 = asyncio.StreamReader(loop=loop)
    r2 = asyncio.StreamReader(loop=loop)

    def protocol_f(reader):
        def f():
            return asyncio.StreamReaderProtocol(reader, loop=loop)
        return f
    (t1, p1), (t2, p2) = create_timeless_transport_pair(protocol_f(r1), protocol_f(r2))
    w1 = asyncio.StreamWriter(t1, p1, r1, loop)
    w2 = asyncio.StreamWriter(t2, p2, r2, loop)
    return (r1, w1), (r2, w2)
