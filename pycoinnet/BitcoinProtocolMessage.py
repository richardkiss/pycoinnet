import binascii
import logging
import io
import struct

from pycoin.block import Block
from pycoin.serialize import bitcoin_streamer
from pycoin.tx.Tx import Tx

from pycoinnet.InvItem import InvItem
from pycoinnet.PeerAddress import PeerAddress


def init_bitcoin_streamer():
    more_parsing = [
        ("A", (PeerAddress.parse, lambda f, peer_addr: peer_addr.stream(f))),
        ("v", (InvItem.parse, lambda f, inv_item: inv_item.stream(f))),
        ("T", (Tx.parse, lambda f, tx: tx.stream(f))),
        ("B", (Block.parse, lambda f, block: block.stream(f))),
        ("b", (lambda f: struct.unpack("?", f.read(1))[0], lambda f, b: f.write(struct.pack("?", b)))),
    ]
    bitcoin_streamer.BITCOIN_STREAMER.register_functions(more_parsing)


### definitions of message structures and types
# L: 4 byte long integer
# Q: 8 byte long integer
# S: unicode string
# [v]: array of InvItem objects
# [LA]: array of (L, PeerAddress) tuples
# b: boolean
# A: PeerAddress object
# B: Block object
# T: Tx object

MESSAGE_STRUCTURES = {
    'version':
        "version:L services:Q timestamp:Q remote_address:A local_address:A"
        " nonce:Q subversion:S last_block_index:L",
    'verack': "",
    'inv': "items:[v]",
    'getdata': "items:[v]",
    'addr': "date_address_tuples:[LA]",
    'alert': "payload signature:SS",
    'tx': "tx:T",
    'block': "block:B",
    'ping': "nonce:Q",
    'pong': "nonce:Q",
}


def _make_parser(the_struct=''):
    def f(message_stream):
        struct_items = [s.split(":") for s in the_struct.split()]
        names = [s[0] for s in struct_items]
        types = ''.join(s[1] for s in struct_items)
        return bitcoin_streamer.parse_as_dict(names, types, message_stream)
    return f


def _message_parsers():
    return dict((k, _make_parser(v)) for k, v in MESSAGE_STRUCTURES.items())


def _message_fixups():
    def fixup_version(d, f):
        if d["version"] >= 70001:
            b = f.read(1)
            if len(b) > 0:
                d["relay"] = (ord(b) != 0)
        return d

    alert_submessage_parser = _make_parser(
        "version:L relayUntil:Q expiration:Q id:L cancel:L setCancel:S minVer:L "
        "maxVer:L setSubVer:S priority:L comment:S statusBar:S reserved:S")

    def fixup_alert(d, f):
        d1 = alert_submessage_parser(f)
        d["alert_info"] = d1
        return d

    return dict(version=fixup_version, alert=fixup_alert)


class BitcoinProtocolMessage(object):
    """
    name: message name
    data: unparsed blob
    """

    MESSAGE_PARSERS = _message_parsers()
    MESSAGE_FIXUPS = _message_fixups()

    @classmethod
    def parse_from_data(class_, message_name, data):
        item = class_()
        item.name = message_name
        message_stream = io.BytesIO(data)
        parser = class_.MESSAGE_PARSERS.get(message_name)
        if parser:
            d = parser(message_stream)
            fixup = class_.MESSAGE_FIXUPS.get(message_name)
            if fixup:
                d = fixup(d, message_stream)
        else:
            logging.error("unknown message: %s %s", message_name, binascii.hexlify(data))
            d = {}
        for k, v in d.items():
            setattr(item, k, v)
        return item

    def __str__(self):
        return "<BitcoinProtocolMessage %s>" % self.name


init_bitcoin_streamer()

