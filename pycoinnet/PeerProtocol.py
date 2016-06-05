import asyncio
import binascii
import logging
import struct
import time

from pycoin import encoding


class ProtocolError(Exception):
    pass


class PeerProtocol(asyncio.Protocol):
    """
    self.connection_made_future:
      a future yielding the transport over which the connection is made
    self.connection_lost_future:
      a future that is set once the connection is lost (or which contains
      the exception that was hit)
    """

    DEFAULT_MAX_MSG_SIZE = 2*1024*1024

    def __init__(self, network, max_msg_size=DEFAULT_MAX_MSG_SIZE, *args, **kwargs):
        super(PeerProtocol, self).__init__(*args, **kwargs)
        self._network = network
        self._magic_header = network.magic_header
        self._parse_from_data = network.parse_from_data
        self._pack_from_data = network.pack_from_data
        self._peername = ("(unconnected)", 0)
        self._connection_made_future = asyncio.Future()
        self._connection_lost_future = asyncio.Future()
        self._msg_lock = asyncio.Lock()
        self._is_writable = False
        # stats
        self._max_msg_size = max_msg_size
        self._bytes_read = 0
        self._bytes_writ = 0
        self._connect_start_time = None

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
        self._transport.write(packet)

    def connection_made(self, transport):
        self._connection_made_future.set_result(transport)
        self._transport = transport
        self._reader = asyncio.StreamReader()
        self._reader.set_transport(transport)
        self._is_writable = True
        self._peername = str(transport)
        self._connect_start_time = time.time()

    def connection_lost(self, exc):
        self._reader.feed_eof()
        if exc:
            self._connection_lost_future.set_exception(exc)
        else:
            self._connection_lost_future.set_result(None)

    def data_received(self, data):
        self._bytes_read += len(data)
        self._reader.feed_data(data)

    def pause_writing(self):
        self._is_writable = False

    def resume_writing(self):
        self._is_writable = True

    def is_writable(self):
        return self._is_writable

    @asyncio.coroutine
    def next_message(self, unpack_to_dict=True):
        with (yield from self._msg_lock):
            # read magic header
            reader = self._reader
            blob = yield from reader.readexactly(len(self._magic_header))
            if blob != self._magic_header:
                raise ProtocolError("bad magic: got %s" % binascii.hexlify(blob))

            # read message name
            message_size_hash_bytes = yield from reader.readexactly(20)
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

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash != transmitted_hash:
            raise ProtocolError("checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash)))
        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))
        if unpack_to_dict:
            message_data = self._parse_from_data(message_name, message_data)
        return message_name, message_data

    def __lt__(self, other):
        return self._connect_start_time < other._connect_start_time

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self._peername)
