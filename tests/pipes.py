
import asyncio
import tempfile


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


@asyncio.coroutine
def create_pipe_streams_pair():
    path = tempfile.mktemp()
    loop = asyncio.get_event_loop()
    server_side = asyncio.Future()

    def factory():

        def client_connected_cb(reader, writer):
            server_side.set_result((reader, writer))
        reader = asyncio.StreamReader(loop=loop)
        return asyncio.StreamReaderProtocol(reader, client_connected_cb, loop=loop)

    server = yield from loop.create_unix_server(factory, path)

    r1 = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(r1, loop=loop)
    transport, _ = yield from loop.create_unix_connection(
        lambda: protocol, path)
    w1 = asyncio.StreamWriter(transport, protocol, r1, loop)

    r2, w2 = yield from server_side
    server.close()
    return (r1, w1), (r2, w2)
