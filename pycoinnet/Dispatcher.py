import asyncio


class Dispatcher:
    def __init__(self, peer):
        self._handlers = dict()
        self._handler_id = 0
        self._peer = peer

    def add_msg_handler(self, msg_handler):
        handler_id = self._handler_id
        self._handlers[handler_id] = msg_handler
        self._handler_id += 1
        return handler_id

    def remove_msg_handler(self, handler_id):
        if handler_id in self._handlers:
            del self._handlers[handler_id]

    def handle_msg(self, name, data):
        loop = asyncio.get_event_loop()
        for m in self._handlers.values():
            # each method gets its own copy of the data dict
            # to protect from it being changed
            data = dict(data)
            if asyncio.iscoroutinefunction(m):
                loop.create_task(m(name, data))
            else:
                m(name, data)

    @asyncio.coroutine
    def wait_for_response(self, *response_types):
        future = asyncio.Future()

        def handle_msg(name, data):
            if response_types and name not in response_types:
                return
            future.set_result((name, data))

        handler_id = self.add_msg_handler(handle_msg)
        future.add_done_callback(lambda f: self.remove_msg_handler(handler_id))
        return (yield from future)

    # TODO: figure out if this should accept a peer (and remove self._peer)
    @asyncio.coroutine
    def handshake(self, **VERSION_MSG):
        # "version"
        self._peer.send_msg("version", **VERSION_MSG)
        msg, version_data = yield from self._peer.next_message()
        self.handle_msg(msg, version_data)
        assert msg == 'version'

        # "verack"
        self._peer.send_msg("verack")
        msg, verack_data = yield from self._peer.next_message()
        self.handle_msg(msg, verack_data)
        assert msg == 'verack'
        return version_data

    @asyncio.coroutine
    def dispatch_messages(self):
        # loop
        try:
            while True:
                msg, data = yield from self._peer.next_message()
                self.handle_msg(msg, data)
        except EOFError:
            pass
        self.handle_msg(None, {})
