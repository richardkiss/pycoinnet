import asyncio
import binascii
import logging
import struct
import time
import weakref

from pycoin import encoding

from pycoinnet.message import parse_from_data, pack_from_data


class BitcoinProtocolError(Exception):
    pass


class BitcoinPeerProtocol(asyncio.Protocol):

    MAX_MESSAGE_SIZE = 2*1024*1024

    def __init__(self, magic_header, *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header
        self.peername = ("(unconnected)", 0)
        self.connection_made_future = asyncio.Future()
        self.connection_lost_future = asyncio.Future()
        self._run_handle = None
        self.message_queues = weakref.WeakSet()
        # stats
        self.bytes_read = 0
        self.bytes_writ = 0
        self.connect_start_time = None
        self._tasks = set()

    def new_get_next_message_f(self, filter_f=lambda message_name, data: True, maxsize=0):
        @asyncio.coroutine
        def run(self):
            yield from asyncio.wait_for(self.connection_made_future, timeout=None)
            while True:
                try:
                    message_name, data = yield from self._parse_next_message()
                except EOFError:
                    logging.debug("end of stream %s", self)
                    message_name, data = None, None
                except Exception:
                    logging.exception("error in _parse_next_message")
                    message_name, data = None, None
                for q in self.message_queues:
                    if q.filter_f(message_name, data):
                        q.put_nowait((message_name, data))
                if message_name is None:
                    break

        q = asyncio.Queue(maxsize=maxsize)
        q.filter_f = filter_f
        self.message_queues.add(q)

        @asyncio.coroutine
        def get_next_message():
            msg_name, data = yield from q.get()
            if msg_name is None:
                raise EOFError
            return msg_name, data

        if self._run_handle:
            if self._run_handle.done():
                q.put_nowait((None, None))
        else:
            self._run_handle = asyncio.Task(run(self))
        return get_next_message

    def add_task(self, task):
        """
        Some Task objects are associated with the peer. This method
        gives an easy way to keep a strong reference to a Task that won't
        disappear until the peer does.
        """
        try:
            self._tasks.add(asyncio.ensure_future(task))
        except AttributeError:
            # compatibility for very old versions of python 3
            self._tasks.add(getattr(asyncio, 'async')(task))

    def send_msg(self, message_name, **kwargs):
        message_data = pack_from_data(message_name, **kwargs)
        message_type = message_name.encode("utf8")
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([
            self.magic_header, message_type_padded, message_size, message_checksum, message_data
        ])
        logging.debug("sending message %s [%d bytes] to %s", message_type.decode("utf8"), len(packet), self)
        self.bytes_writ += len(packet)
        self.transport.write(packet)

    def connection_made(self, transport):
        self.connection_made_future.set_result(transport)
        self.transport = transport
        self.reader = asyncio.StreamReader()
        self._is_writable = True
        self.peername = transport.get_extra_info("socket").getpeername()
        self.connect_start_time = time.time()

    def connection_lost(self, exc):
        if exc:
            self.connection_lost_future.set_exception(exc)
        else:
            self.connection_lost_future.set_result(None)
        # ## TODO: fix this. This is a stupid way of doing it.
        # What is the correct way to shut this down?
        self.reader.feed_eof()

    def data_received(self, data):
        self.bytes_read += len(data)
        self.reader.feed_data(data)

    def pause_writing(self):
        self._is_writable = False

    def resume_writing(self):
        self._is_writable = True

    def is_writable(self):
        return self._is_writable

    @asyncio.coroutine
    def _parse_next_message(self):

        # read magic header
        reader = self.reader
        blob = yield from reader.readexactly(len(self.magic_header))
        if blob != self.magic_header:
            raise BitcoinProtocolError("bad magic: got %s" % binascii.hexlify(blob))

        # read message name
        message_size_hash_bytes = yield from reader.readexactly(20)
        message_name_bytes = message_size_hash_bytes[:12]
        message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

        # get size of message
        size_bytes = message_size_hash_bytes[12:16]
        size = int.from_bytes(size_bytes, byteorder="little")
        if size > self.MAX_MESSAGE_SIZE:
            raise BitcoinProtocolError("absurdly large message size %d" % size)

        # read the hash, then the message
        transmitted_hash = message_size_hash_bytes[16:20]
        message_data = yield from reader.readexactly(size)

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash != transmitted_hash:
            raise BitcoinProtocolError("checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash)))
        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))

        # parse the blob into a BitcoinProtocolMessage object
        data = parse_from_data(message_name, message_data)
        return message_name, data

    def __lt__(self, other):
        return self.connect_start_time < other.connect_start_time

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.peername)
