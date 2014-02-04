import asyncio
import binascii
import logging
import os
import struct
import time

from pycoin import encoding

from pycoinnet.message import parse_from_data, pack_from_data
from pycoinnet.message import MESSAGE_STRUCTURES
from pycoinnet.PeerAddress import PeerAddress


ITEM_TYPE_TX, ITEM_TYPE_BLOCK = (1, 2)


class BitcoinProtocolError(Exception):
    pass


class BitcoinPeerProtocol(asyncio.Protocol):

    HANDLE_MESSAGE_NAMES = ["msg_%s" % msg_name for msg_name in MESSAGE_STRUCTURES.keys()]
    HANDLE_MESSAGE_NAMES.extend(["msg", "connection_made", "connection_lost"])

    MAX_MESSAGE_SIZE = 2*1024*1024

    def __init__(self, magic_header, *args, **kwargs):
        super(BitcoinPeerProtocol, self).__init__(*args, **kwargs)
        self.magic_header = magic_header
        self.delegate_methods = dict((event, []) for event in self.HANDLE_MESSAGE_NAMES)
        self.override_msg_version_parameters = {}
        self.peername = "(unconnected)"
        self.handshake_complete = asyncio.Future()
        ## stats
        self.bytes_read = 0
        self.bytes_writ = 0
        self.connect_start_time = None

    def register_delegate(self, delegate):
        """
        Call this method with your delegate, and any methods it contains
        named handle_(event) where event is from the list HANDLE_MESSAGE_NAMES
        will be invoked when the event occurs or message comes in.

        The first parameter will be the BitcoinPeerProtocol (the "self"
        here, so your delegate can optionally manage many peers).
        """
        for event in self.HANDLE_MESSAGE_NAMES:
            method_name = "handle_%s" % event
            if hasattr(delegate, method_name):
                self.delegate_methods[event].append(getattr(delegate, method_name))

    def unregister_delegate(self, delegate):
        for event in self.HANDLE_MESSAGE_NAMES:
            method_name = "handle_%s" % event
            if hasattr(delegate, method_name):
                self.delegate_methods[event].remove(getattr(delegate, method_name))

    def default_msg_version_parameters(self):
        remote_ip, remote_port = self.peername
        remote_addr = PeerAddress(1, remote_ip, remote_port)
        local_addr = PeerAddress(1, "127.0.0.1", 6111)
        d = dict(
            version=70001, subversion=b"/Notoshi/", services=1, timestamp=int(time.time()),
            remote_address=remote_addr, local_address=local_addr,
            nonce=struct.unpack("!Q", os.urandom(8))[0],
            last_block_index=0, want_relay=True
        )
        return d

    def update_msg_version_parameters(self, d):
        """
        Use this method to override any parameters included in the initial
        version message (see messages.py). You obviously must call this before it's sent.
        """
        self.override_msg_version_parameters.update(d)

    def connection_made(self, transport):
        self.transport = transport
        self.reader = asyncio.StreamReader()
        self._request_handle = asyncio.async(self.run())
        self.trigger_event("connection_made", dict(transport=transport))
        self._is_writable = True
        self.peername = transport.get_extra_info("socket").getpeername()
        self.connect_start_time = time.time()

    def connection_lost(self, exc):
        self._request_handle.cancel()
        self.trigger_event("connection_lost", dict(exc=exc))

    def send_msg(self, message_name, **kwargs):
        message_data = pack_from_data(message_name, **kwargs)
        message_type = message_name.encode("utf8")
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([
            self.magic_header, message_type_padded, message_size, message_checksum, message_data
        ])
        logging.debug("sending message %s [%d bytes]", message_type.decode("utf8"), len(packet))
        self.bytes_writ += len(packet)
        self.transport.write(packet)

    def trigger_event(self, event, data):
        if event in self.delegate_methods:
            methods = self.delegate_methods.get(event)
            if methods:
                for m in methods:
                    m(self, **data)
        else:
            logging.error("unknown event %s", event)

    @asyncio.coroutine
    def run(self):
        try:
            # do handshake

            self.is_running = True

            ## BRAIN DAMAGE: this should be better
            d = self.default_msg_version_parameters()
            d.update(self.override_msg_version_parameters)
            self.send_msg("version", **d)

            message_name, version_data = yield from self.parse_next_message()
            if message_name != 'version':
                raise BitcoinProtocolError("missing version")
            self.trigger_event("msg_%s" % message_name, version_data)
            self.send_msg("verack")

            message_name, data = yield from self.parse_next_message()
            if message_name != 'verack':
                raise BitcoinProtocolError("missing verack")
            self.trigger_event("msg_%s" % message_name, data)

            self.handshake_complete.set_result(version_data)

            while self.is_running:
                message_name, data = yield from self.parse_next_message()
                self.trigger_event("msg_%s" % message_name, data)

        except Exception:
            logging.exception("message parse failed in %s", self.peername)

        self.transport.close()

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

        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))

        # parse the blob into a BitcoinProtocolMessage object
        data = parse_from_data(message_name, message_data)
        self.trigger_event("msg", dict(message_name=message_name, data=data))
        return message_name, data

    def stop(self):
        self.transport.close()
        ## is this necessary?
        #self._request_handle.cancel()

    def data_received(self, data):
        self.bytes_read += len(data)
        self.reader.feed_data(data)

    def pause_writing(self):
        self._is_writable = False

    def resume_writing(self):
        self._is_writable = True

    def is_writable(self):
        return self._is_writable

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "<Peer %s>" % str(self.peername)
