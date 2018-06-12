import asyncio
import logging

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer


def parser_and_packer_for_network(network):
    # BRAIN DAMAGE
    from pycoin.message.make_parser_and_packer import (
        make_parser_and_packer, standard_messages,
        standard_message_post_unpacks, standard_streamer, standard_parsing_functions
    )

    streamer = standard_streamer(standard_parsing_functions(network.block, network.tx))
    parser, packer = make_parser_and_packer(
        streamer, standard_messages(), standard_message_post_unpacks(streamer))

    def new_parser(message, data):
        try:
            return parser(message, data)
        except KeyError:
            logging.error("unknown message %s", message)
            return b''

    return new_parser, packer


def peer_connect_pipeline(network, tcp_connect_workers=30, handshake_workers=3,
                          host_q=None, loop=None, version_dict={}):

    host_q = host_q or dns_bootstrap_host_port_q(network)

    async def do_tcp_connect(host_port_pair, q):
        host, port = host_port_pair
        logging.debug("TCP connecting to %s:%d", host, port)
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            logging.debug("TCP connected to %s:%d", host, port)
            await q.put((reader, writer))
        except Exception as ex:
            logging.info("connect failed: %s:%d (%s)", host, port, ex)

    async def do_peer_handshake(rw_tuple, q):
        reader, writer = rw_tuple
        parse_from_data, pack_from_data = parser_and_packer_for_network(network)
        peer = Peer(
            reader, writer, network.magic_header, parse_from_data,
            pack_from_data, max_msg_size=10*1024*1024)
        version_data = version_data_for_peer(peer, **version_dict)
        peer.version = await peer.perform_handshake(**version_data)
        if peer.version is None:
            logging.info("handshake failed on %s", peer)
            peer.close()
        else:
            await q.put(peer)

    filters = [
        dict(callback_f=do_tcp_connect, input_q=host_q, worker_count=tcp_connect_workers),
        dict(callback_f=do_peer_handshake, worker_count=handshake_workers),
    ]
    return MappingQueue(*filters, loop=loop)


def get_peer_pipeline(network, peer_addresses, peer_q):
    # for now, let's just do one peer
    host_q = None
    if peer_addresses:
        host_q = asyncio.Queue()
        for peer in peer_addresses:
            if ":" in peer:
                host, port = peer.split(":", 1)
                port = int(port)
            else:
                host = peer
                port = network.default_port
            host_q.put_nowait((host, port))
    # BRAIN DAMAGE: 70016 version number is required for bgold new block header format
    return peer_connect_pipeline(network, host_q=host_q, version_dict=dict(version=70015))
