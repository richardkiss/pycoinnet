
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
