
import asyncio
import tempfile


class NullModemTransport(asyncio.ReadTransport, asyncio.WriteTransport):
    """
    This is a piped transport that is a reader and a writer.
    Perfect for testing.
    """
    def __init__(self, protocol, loop=None, extra_info=dict(), latency=0.0, write_buffer_size=1024*1024):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._extra_info = extra_info
        self._latency = latency
        self._is_closing = False
        self._closed = False
        self._loop = loop
        self._protocol = protocol
        self._write_buffer_size = write_buffer_size
        self._buffer = []
        self._connection_exc = None
        loop.call_soon(self._protocol.connection_made, self)

    def _move_data(self):
        if self._reading_is_paused:
            return
        b = bytearray()
        while len(self._buffer) > 0:
            d, self._buffer = self._buffer[0], self._buffer[1:]
            b += d
        if len(b) > 0:
            self._protocol.data_received(bytes(b))
        if self._is_closing:
            self._protocol.connection_lost(self._connection_exc)

    ###########################

    def close(self):
        self._is_closing = False
        self._closed = True

    def get_extra_info(self):
        return self._extra_info

    def is_closing(self):
        return self._closed or self._is_closing

    # asyncio.ReadTransport

    def pause_reading(self):
        self._reading_is_paused = True

    def resume_reading(self):
        self._reading_is_paused = False
        self._move_data()

    # asyncio.WriteTransport

    def abort(self):
        self.write_eof()

    def can_write_eof(self):
        return True

    def get_write_buffer_size(self):
        return self._write_buffer_size

    def get_write_buffer_limits(self):
        return self._low, self._high

    def set_write_buffer_limits(self, high=None, low=None):
        self._high = high
        self._low = low

    def write(self, data):
        self._buffer.append(data)
        self._loop.call_later(self._latency, self._move_data)

    def write_eof(self):
        self.is_closing = True
        self._loop.call_later(self._latency, self._move_data)

    # clever ideas for the future
    def set_latency(self, latency_seconds):
        self._latency = latency_seconds

    def set_bandwith(self, bandwidth_bytes_per_second):
        self._bandwith = bandwidth_bytes_per_second


@asyncio.coroutine
def create_pipe_pairs(protocol_factory):
    path = tempfile.mktemp()
    loop = asyncio.get_event_loop()
    server_side = asyncio.Future()

    def server_callback():
        p = protocol_factory()
        server_side.set_result(p)
        return p

    server = yield from loop.create_unix_server(server_callback, path)
    t1, p1 = yield from loop.create_unix_connection(protocol_factory, path)
    p2 = yield from server_side
    server.close()
    return p1, p2
