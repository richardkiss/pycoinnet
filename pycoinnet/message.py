import binascii
import logging
import io
import struct

from pycoin.block import Block, BlockHeader
from pycoin.encoding import double_sha256
from pycoin.serialize import b2h_rev, bitcoin_streamer
from pycoin.tx.Tx import Tx

from pycoinnet.InvItem import InvItem
from pycoinnet.PeerAddress import PeerAddress

# definitions of message structures and types
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
    'version': (
        "version:L services:Q timestamp:Q remote_address:A local_address:A"
        " nonce:Q subversion:S last_block_index:L"
    ),
    'verack': "",
    'addr': "date_address_tuples:[LA]",
    'inv': "items:[v]",
    'getdata': "items:[v]",
    'notfound': "items:[v]",
    'getblocks': "version:L hashes:[#] hash_stop:#",
    'getheaders': "version:L hashes:[#] hash_stop:#",
    'tx': "tx:T",
    'block': "block:B",
    'headers': "headers:[zI]",
    'getaddr': "",
    'mempool': "",
    # 'checkorder': obsolete
    # 'submitorder': obsolete
    # 'reply': obsolete
    'ping': "nonce:Q",
    'pong': "nonce:Q",
    'filterload': "filter:[1] hash_function_count:L tweak:L flags:b",
    'filteradd': "data:[1]",
    'filterclear': "",
    'merkleblock': (
        "header:z total_transactions:L hashes:[#] flags:[1]"
    ),
    'alert': "payload:S signature:S",
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


def fixup_merkleblock(d, f):
    def recurse(level_widths, level_index, node_index, hashes, flags, flag_index, tx_acc):
        idx, r = divmod(flag_index, 8)
        mask = (1 << r)
        flag_index += 1
        if flags[idx] & mask == 0:
            h = hashes.pop()
            return h, flag_index

        if level_index == len(level_widths) - 1:
            h = hashes.pop()
            tx_acc.append(h)
            return h, flag_index

        # traverse the left
        left_hash, flag_index = recurse(
            level_widths, level_index+1, node_index*2, hashes, flags, flag_index, tx_acc)

        # is there a right?
        if node_index*2+1 < level_widths[level_index+1]:
            right_hash, flag_index = recurse(
                level_widths, level_index+1, node_index*2+1, hashes, flags, flag_index, tx_acc)

            if left_hash == right_hash:
                raise ValueError("merkle hash has same left and right value at node %d" % node_index)
        else:
            right_hash = left_hash

        return double_sha256(left_hash + right_hash), flag_index

    level_widths = []
    count = d["total_transactions"]
    while count > 1:
        level_widths.append(count)
        count += 1
        count //= 2
    level_widths.append(1)
    level_widths.reverse()

    tx_acc = []
    flags = d["flags"]
    hashes = list(reversed(d["hashes"]))
    left_hash, flag_index = recurse(level_widths, 0, 0, hashes, flags, 0, tx_acc)

    if len(hashes) > 0:
        raise ValueError("extra hashes: %s" % hashes)

    idx, r = divmod(flag_index-1, 8)
    if idx != len(flags) - 1:
        raise ValueError("not enough flags consumed")

    if flags[idx] > (1 << (r+1))-1:
        raise ValueError("unconsumed 1 flag bits set")

    if left_hash != d["header"].merkle_root:
        raise ValueError(
            "merkle root %s does not match calculated hash %s" % (
                b2h_rev(d["header"].merkle_root), b2h_rev(left_hash)))

    d["tx_hashes"] = tx_acc
    return d


def _message_fixups():
    def fixup_version(d, f):
        if d["version"] >= 70001:
            b = f.read(1)
            if len(b) > 0:
                d["relay"] = (ord(b) != 0)
        return d

    alert_submessage_parser = _make_parser(
        "version:L relayUntil:Q expiration:Q id:L cancel:L setCancel:[L] minVer:L "
        "maxVer:L setSubVer:[S] priority:L comment:S statusBar:S reserved:S")

    def fixup_alert(d, f):
        d1 = alert_submessage_parser(io.BytesIO(d["payload"]))
        d["alert_info"] = d1
        return d

    return dict(version=fixup_version, alert=fixup_alert, merkleblock=fixup_merkleblock)


def _make_parse_from_data():

    def init_bitcoin_streamer():
        more_parsing = [
            ("A", (PeerAddress.parse, lambda f, peer_addr: peer_addr.stream(f))),
            ("v", (InvItem.parse, lambda f, inv_item: inv_item.stream(f))),
            ("T", (Tx.parse, lambda f, tx: tx.stream(f))),
            ("B", (Block.parse, lambda f, block: block.stream(f))),
            ("z", (BlockHeader.parse, lambda f, blockheader: blockheader.stream(f))),
            ("1", (lambda f: struct.unpack("B", f.read(1))[0], lambda f, b: f.write(struct.pack("B", b)))),
        ]
        bitcoin_streamer.BITCOIN_STREAMER.register_functions(more_parsing)

    init_bitcoin_streamer()

    MESSAGE_PARSERS = _message_parsers()
    MESSAGE_FIXUPS = _message_fixups()

    def parse_from_data(message_name, data):
        message_stream = io.BytesIO(data)
        parser = MESSAGE_PARSERS.get(message_name)
        if parser:
            d = parser(message_stream)
            fixup = MESSAGE_FIXUPS.get(message_name)
            if fixup:
                d = fixup(d, message_stream)
        else:
            logging.error("unknown message: %s %s", message_name, binascii.hexlify(data))
            d = {}
        return d
    return parse_from_data


parse_from_data = _make_parse_from_data()


def pack_from_data(message_name, **kwargs):
    the_struct = MESSAGE_STRUCTURES[message_name]
    if not the_struct:
        return b''
    f = io.BytesIO()
    the_fields = the_struct.split(" ")
    pairs = [t.split(":") for t in the_fields]
    for name, type in pairs:
        if type[0] == '[':
            bitcoin_streamer.BITCOIN_STREAMER.stream_struct("I", f, len(kwargs[name]))
            for v in kwargs[name]:
                if not isinstance(v, (tuple, list)):
                    v = [v]
                bitcoin_streamer.BITCOIN_STREAMER.stream_struct(type[1:-1], f, *v)
        else:
            bitcoin_streamer.BITCOIN_STREAMER.stream_struct(type, f, kwargs[name])
    return f.getvalue()
