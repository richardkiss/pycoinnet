import asyncio.queues
import binascii
import logging
import struct
import time

from pycoin import encoding
from pycoin.serialize import bitcoin_streamer

from pycoinnet.BitcoinProtocolMessage import BitcoinProtocolMessage
from pycoinnet.PeerAddress import PeerAddress


class BitcoinProtocolError(Exception):
    pass


class BitcoinPeerProtocol(asyncio.Protocol):

    def __init__(self, magic_header=binascii.unhexlify('0B110907'), *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header

    def connection_made(self, transport):
        logging.debug("connection made %s", transport)
        self.transport = transport
        self.reader = asyncio.StreamReader()
        self.messages = asyncio.queues.Queue()
        self.last_message_timestamp = time.time()
        self._request_handle = asyncio.async(self.start())

    def data_received(self, data):
        self.reader.feed_data(data)

    def send_message(self, message_type, message_data=b''):
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([self.magic_header, message_type_padded, message_size, message_checksum, message_data])
        logging.debug("sending message %s [%d bytes]", message_type.decode("utf8"), len(packet))
        self.transport.write(packet)

    @asyncio.coroutine
    def start(self):
        while True:
            try:
                yield from self.parse_next_message()
                self.last_message_timestamp = time.time()
            except Exception:
                logging.exception("message parse failed")

    MAX_MESSAGE_SIZE = 20*1024*1024

    @asyncio.coroutine
    def parse_next_message(self):

        # read magic header
        reader = self.reader
        blob = yield from reader.readexactly(len(self.magic_header))
        if blob != self.magic_header:
            s = "bad magic: got %s" % binascii.hexlify(blob)
            logging.error(s)
            raise BitcoinProtocolError(s)

        # read message name
        message_name_bytes = yield from reader.readexactly(12)
        message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

        # get size of message
        size_bytes = yield from reader.readexactly(4)
        size = int.from_bytes(size_bytes, byteorder="little")
        if size > self.MAX_MESSAGE_SIZE:
            raise BitcoinProtocolError("absurdly large message size %d" % size)

        # read the hash, then the message
        transmitted_hash = yield from reader.readexactly(4)
        message_data = yield from reader.readexactly(size)

        # check the hash
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash == transmitted_hash:
            logging.debug("checksum is CORRECT")
        else:
            s = "checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash))
            logging.error(s)
            raise BitcoinProtocolError(s)

        logging.debug("message: %s (%d byte payload)", message_name, len(message_data))

        # parse the blob into a BitcoinProtocolMessage object
        msg = BitcoinProtocolMessage.parse_from_data(message_name, message_data)
        yield from self.messages.put(msg)

    def next_message(self):
        return self.messages.get()

    def send_msg_version(
            self, version, subversion, services, current_time, remote_address,
            remote_listen_port, local_address, local_listen_port, nonce, last_block_index, want_relay):
        remote = PeerAddress(1, remote_address, remote_listen_port)
        local = PeerAddress(1, local_address, local_listen_port)
        the_bytes = bitcoin_streamer.pack_struct(
            "LQQAAQSL", version, services, current_time,
            remote, local, nonce, subversion, last_block_index)
        self.send_message(b"version", the_bytes)

    def send_msg_verack(self):
        self.send_message(b"verack")

    def send_msg_getaddr(self):
        self.send_message(b"getaddr")

    def send_msg_getdata(self, items):
        the_bytes = bitcoin_streamer.pack_struct("I" + ("v" * len(items)), len(items), *items)
        self.send_message(b"getdata", the_bytes)

    def send_msg_ping(self, nonce):
        nonce_data = struct.pack("<Q", nonce)
        self.send_message(b"ping", nonce_data)

    def send_msg_pong(self, nonce):
        nonce_data = struct.pack("<Q", nonce)
        self.send_message(b"pong", nonce_data)
