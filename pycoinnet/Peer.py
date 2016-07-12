import asyncio
import binascii
import logging
import struct

from pycoin import encoding


class ProtocolError(Exception):
    pass


class Peer:
    DEFAULT_MAX_MSG_SIZE = 2*1024*1024

    def __init__(self, stream_reader, stream_writer, magic_header,
                 parse_from_data_f, pack_from_data_f, max_msg_size=DEFAULT_MAX_MSG_SIZE):
        self._reader = stream_reader
        self._writer = stream_writer
        self._magic_header = magic_header
        self._parse_from_data = parse_from_data_f
        self._pack_from_data = pack_from_data_f
        self._max_msg_size = max_msg_size
        self._msg_lock = asyncio.Lock()
        # stats
        self._bytes_read = 0
        self._bytes_writ = 0
        self._handler_id = 0
        self._handlers = dict()
        self._dispatcher_task = None

    def send_msg(self, message_name, **kwargs):
        message_data = self._pack_from_data(message_name, **kwargs)
        message_type = message_name.encode("utf8")
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([
            self._magic_header, message_type_padded, message_size, message_checksum, message_data
        ])
        logging.debug("sending message %s [%d bytes] to %s", message_type.decode("utf8"), len(packet), self)
        self._bytes_writ += len(packet)
        self._writer.write(packet)

    @asyncio.coroutine
    def next_message(self, unpack_to_dict=True):
        header_size = len(self._magic_header)
        with (yield from self._msg_lock):
            # read magic header
            reader = self._reader
            blob = yield from reader.readexactly(header_size)
            self._bytes_read += header_size
            if blob != self._magic_header:
                raise ProtocolError("bad magic: got %s" % binascii.hexlify(blob))

            # read message name
            message_size_hash_bytes = yield from reader.readexactly(20)
            self._bytes_read += 20
            message_name_bytes = message_size_hash_bytes[:12]
            message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

            # get size of message
            size_bytes = message_size_hash_bytes[12:16]
            size = int.from_bytes(size_bytes, byteorder="little")
            if size > self._max_msg_size:
                raise ProtocolError("absurdly large message size %d" % size)

            # read the hash, then the message
            transmitted_hash = message_size_hash_bytes[16:20]
            message_data = yield from reader.readexactly(size)
            self._bytes_read += size

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash != transmitted_hash:
            raise ProtocolError("checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash)))
        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))
        if unpack_to_dict:
            message_data = self._parse_from_data(message_name, message_data)
        return message_name, message_data

    @asyncio.coroutine
    def perform_handshake(self, **VERSION_MSG):
        # "version"
        self.send_msg("version", **VERSION_MSG)
        msg, version_data = yield from self.next_message()
        self.handle_msg(msg, version_data)
        assert msg == 'version'

        # "verack"
        self.send_msg("verack")
        msg, verack_data = yield from self.next_message()
        self.handle_msg(msg, verack_data)
        assert msg == 'verack'
        return version_data

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

    def write_eof(self):
        self._writer.write_eof()

    @asyncio.coroutine
    def _dispatch_messages(self):
        try:
            while True:
                msg, data = yield from self.next_message()
                self.handle_msg(msg, data)
        except EOFError:
            pass
        self.handle_msg(None, {})
        self.close()

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

    def start_dispatcher(self):
        if self._dispatcher_task is not None:
            raise RuntimeError("dispatcher already started")
        self._dispatcher_task = asyncio.get_event_loop().create_task(self._dispatch_messages())

    def close(self):
        self._writer.close()

    def is_closing(self):
        return self._writer._transport.is_closing()

    def __repr__(self):
        peer_info = self._reader._transport.get_extra_info("peername")
        if peer_info is None:
            peer_info = self._reader._transport
        return "<Peer %s>" % str(peer_info)
