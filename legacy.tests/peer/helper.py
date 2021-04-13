import asyncio
import hashlib
import logging

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.helpers.standards import initial_handshake, version_data_for_peer
from pycoinnet.PeerAddress import PeerAddress

from pycoin import ecdsa
from pycoin.block import Block
from pycoin.encoding import public_pair_to_sec
from pycoin.tx.Tx import Tx, TxIn, TxOut

MAGIC_HEADER = b"food"

class PeerTransport(asyncio.Transport):
    def __init__(self, write_f, peer_name=("192.168.1.1", 8081), *args, **kwargs):
        super(PeerTransport, self).__init__(*args, **kwargs)
        self.write_f = write_f
        self.peer_name = peer_name
        self.writ_data = bytearray()

    def write(self, data):
        self.write_f(data)
        self.writ_data.extend(data)

    def close(self):
        pass

    def get_extra_info(self, key):
        class ob:
            def getpeername(inner_self):
                return self.peer_name
        return ob()


VERSION_MSG_BIN = b'foodversion\x00\x00\x00\x00\x00^\x00\x00\x00\xe0?\xce\xd8q\x11\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00"\xd7\x03S\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x02\x17\xdf\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\x7f\x00\x00\x01\x17\xdf\xec\r#\xbb\x82 Z/\t/Notoshi/\x00\x00\x00\x00'

VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=0
)

VERACK_MSG_BIN = b'foodverack\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00]\xf6\xe0\xe2'

VERSION_MSG_2 = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760614,
    remote_address=PeerAddress(1, "127.0.0.1", 6111),
    local_address=PeerAddress(1, "127.0.0.2", 6111),
    nonce=5412937754643071,
    last_block_index=0
)

def dump(peer):
    for msg in peer.msg_list:
        print(msg)

def watch_messages(peer):
    @asyncio.coroutine
    def _watch(msg_list, next_message_f):
        while True:
            v = yield from next_message_f()
            msg_list.append(v)
            if v[0] == None:
                break
    peer.msg_list = []
    peer.add_task(_watch(peer.msg_list, peer.new_get_next_message_f()))
    peer.dump = lambda: dump(peer)

def create_peers(ip1="127.0.0.1", ip2="127.0.0.2"):
    peer1 = BitcoinPeerProtocol(MAGIC_HEADER)
    peer2 = BitcoinPeerProtocol(MAGIC_HEADER)

    pt1 = PeerTransport(peer2.data_received, (ip2, 6111))
    pt2 = PeerTransport(peer1.data_received, (ip1, 6111))

    peer1.writ_data = pt1.writ_data
    peer2.writ_data = pt2.writ_data

    # connect them
    peer1.connection_made(pt1)
    peer2.connection_made(pt2)
    return peer1, peer2

def handshake_peers(peer1, peer2, peer_info_1={}, peer_info_2={}):
    msg1 = version_data_for_peer(peer1, **peer_info_1)
    msg2 = version_data_for_peer(peer2, **peer_info_2)
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, msg1), initial_handshake(peer2, msg2)]))
    return peer1, peer2

def create_handshaked_peers(ip1="127.0.0.1", ip2="127.0.0.2"):
    peer1, peer2 = create_peers(ip1=ip1, ip2=ip2)
    watch_messages(peer1)
    watch_messages(peer2)
    asyncio.get_event_loop().run_until_complete(asyncio.wait([initial_handshake(peer1, VERSION_MSG), initial_handshake(peer2, VERSION_MSG_2)]))
    return peer1, peer2

def create_peers_tcp():
    @asyncio.coroutine
    def run_listener():
        abstract_server = None
        port = 60661
        future_peer = asyncio.Future()
        def protocol_factory():
            peer = BitcoinPeerProtocol(MAGIC_HEADER)
            future_peer.set_result(peer)
            #abstract_server.close()
            return peer
        while abstract_server is None:
            try:
                abstract_server = yield from asyncio.get_event_loop().create_server(protocol_factory=protocol_factory, port=port)
            except Exception as OSError:
                port += 1
        return abstract_server, port, future_peer

    server, port, future_peer = asyncio.get_event_loop().run_until_complete(asyncio.Task(run_listener()))

    @asyncio.coroutine
    def run_connector(port):
        def protocol_factory():
            return BitcoinPeerProtocol(MAGIC_HEADER)
        transport, protocol = yield from asyncio.get_event_loop().create_connection(
            protocol_factory, host="127.0.0.1", port=port)
        logging.debug("connected on port %s", port)
        return protocol

    peer2 = asyncio.get_event_loop().run_until_complete(asyncio.Task(run_connector(port)))
    peer1 = asyncio.get_event_loop().run_until_complete(future_peer)

    watch_messages(peer1)
    watch_messages(peer2)

    return peer1, peer2
