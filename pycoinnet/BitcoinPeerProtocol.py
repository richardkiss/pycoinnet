import asyncio
import binascii
import datetime
import io
import logging
import os
import struct
import time

from pycoin.serialize import bitcoin_streamer
from pycoinnet.BitcoinPeerStreamReader import BitcoinPeerStreamReader
from pycoinnet.reader import init_bitcoin_streamer
from pycoinnet.reader.PeerAddress import PeerAddress

init_bitcoin_streamer()


class BitcoinPeerProtocol(asyncio.Protocol):

    def connection_made(self, transport):
        logging.debug("connection made %s", transport)
        self.transport = transport
        self.stream = BitcoinPeerStreamReader()
        self._request_handle = asyncio.Task(self.start())

    def data_received(self, data):
        self.stream.feed_data(data)

    def write_message(self, message_type, message_data=b''):
        packet = self.stream.packet_for_message(message_type, message_data)
        logging.debug("sending message %s [%d bytes]", message_type, len(packet))
        self.transport.write(packet)

    def get_msg_version_parameters_default(self):
        # this must return a dictionary with:
        #  version (integer)
        #  subversion (bytes, like b"/Satoshi:0.7.2/")
        #  node_type (must be 1)
        #  current time (seconds since epoch)
        #  remote_address
        #  remote_listen_port
        #  local_address
        #  local_listen_port
        #  nonce (32 bit)
        #  last_block_index
        remote_address, remote_port = self.transport.get_extra_info("socket").getpeername()
        return dict(
            version=70001,
            subversion=b"/Notoshi/",
            node_type=1,
            current_time=int(time.time()),
            remote_address=remote_address,
            remote_listen_port=remote_port,
            local_address="127.0.0.1",
            local_listen_port=6111,
            nonce=struct.unpack("!Q", os.urandom(8))[0],
            last_block_index=0,
        )

    def get_msg_version_parameters(self):
        return {}

    @asyncio.coroutine
    def start(self):
        d = self.get_msg_version_parameters_default()
        d.update(self.get_msg_version_parameters())
        self.send_msg_version(**d)

        self.ping_nonce_handle_lookup = {}
        while True:
            # schedule ping
            try:
                ping_handle = asyncio.get_event_loop().call_later(60, self.do_ping)
                yield from self.parse_next_message()
                ping_handle.cancel()
            except Exception:
                logging.exception("message parse failed")

    def parse_next_message(self):
        message_name, message_data = yield from self.stream.readmessage()

        logging.debug("message: %s (%d byte payload)", message_name, len(message_data))

        def version_supplement(d):
            d["when"] = datetime.datetime.fromtimestamp(d["timestamp"])

        def alert_supplement(d):
            d["alert_msg"] = bitcoin_streamer.parse_as_dict(
                "version relayUntil expiration id cancel setCancel minVer maxVer setSubVer priority comment statusBar reserved".split(),
                "LQQLLSLLSLSSS",
                d["payload"])

        PARSE_PAIR = {
            'version': (
                "version node_type timestamp remote_address local_address nonce subversion last_block_index",
                "LQQAAQSL",
                version_supplement
            ),
            'verack': ("", ""),
            'inv': ("items", "[v]"),
            'getdata': ("items", "[v]"),
            'addr': ("date_address_tuples", "[LA]"),
            'alert': ("payload signature", "SS", alert_supplement),
            'tx': ("tx", "T"),
            'block': ("block", "B"),
            'pong': ("nonce", "L"),
        }

        the_tuple = PARSE_PAIR.get(message_name)
        if the_tuple is None:
            logging.error("unknown message: %s %s", message_name, binascii.hexlify(message_data))
        else:
            prop_names, prop_struct = the_tuple[:2]
            post_f = lambda d: 0
            if len(the_tuple) > 2:
                post_f = the_tuple[2]
            d = bitcoin_streamer.parse_as_dict(prop_names.split(), prop_struct, io.BytesIO(message_data))
            post_f(d)
            f = getattr(self, "handle_msg_%s" % message_name)
            f(**d)

    def do_ping(self):
        def pong_missing():
            logging.debug("pong missing with nonce %s, disconnecting", binascii.hexlify(nonce))
            self.transport.close()

        nonce = os.urandom(8)
        self.send_msg_ping(nonce)
        logging.debug("sending ping with nonce %s", binascii.hexlify(nonce))
        # look for a pong with the given nonce
        handle = asyncio.get_event_loop().call_later(60, pong_missing)
        self.ping_nonce_handle_lookup[nonce] = handle

    def handle_msg_pong(self, nonce):
        logging.debug("got pong with nonce %s", binascii.unhexlify(nonce))
        if nonce in self.ping_nonce_handle_lookup:
            logging.debug("canceling hang-up")
            self.ping_nonce_handle_lookup[nonce].cancel()
            del self.ping_nonce_handle_lookup[nonce]

    def handle_msg_addr(self, date_address_tuples):
        logging.info("got addresses %s", str(date_address_tuples))

    def handle_msg_version(self, version, node_type, timestamp, remote_address, local_address, nonce, subversion, last_block_index, when):
        self.send_msg_verack()

    def handle_msg_verack(self):
        pass

    def handle_msg_inv(self, items):
        pass

    def handle_msg_alert(self, payload, signature, alert_msg):
        pass

    def handle_msg_tx(self, tx):
        pass

    def send_msg_version(self, version, subversion, node_type, current_time, remote_address, remote_listen_port, local_address, local_listen_port, nonce, last_block_index):
        remote = PeerAddress(1, remote_address, remote_listen_port)
        local = PeerAddress(1, local_address, local_listen_port)
        the_bytes = bitcoin_streamer.pack_struct("LQQAAQSL", version, node_type, current_time, remote, local, nonce, subversion, last_block_index)
        self.write_message(b"version", the_bytes)

    def send_msg_verack(self):
        self.write_message(b"verack", b'')

    def send_msg_getdata(self, items):
        the_bytes = bitcoin_streamer.pack_struct("I" + ("v" * len(items)), len(items), *items)
        self.write_message(b"getdata", the_bytes)

    def send_msg_ping(self, nonce):
        self.write_message(b"ping", nonce)
