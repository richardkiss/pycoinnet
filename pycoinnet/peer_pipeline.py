import asyncio
import logging

from pycoinnet.aitertools import iter_to_aiter, parallel_map_aiter, flatten_aiter
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_iterator
from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer


def peer_connect_iterator(network, tcp_connect_workers=30, handshake_workers=3,
                          host_iterator=None, loop=None, version_dict={}):

    if host_iterator is None:
        host_iterator = dns_bootstrap_host_port_iterator(network)

    result_q = asyncio.Queue()

    async def host_port_to_reader_writer(host_port_pair):
        host, port = host_port_pair
        logging.debug("TCP connecting to %s:%d", host, port)
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            logging.debug("TCP connected to %s:%d", host, port)
            return [(reader, writer)]
        except Exception as ex:
            logging.info("connect failed: %s:%d (%s)", host, port, ex)

    async def peer_handshake(reader_writer):
        reader, writer = reader_writer
        peer = Peer(
            reader, writer, network.magic_header, network.parse_message,
            network.pack_message, max_msg_size=10*1024*1024)
        version_data = version_data_for_peer(peer, **version_dict)
        peer.version = await perform_handshake(peer, **version_data)
        if peer.version is None:
            logging.info("handshake failed on %s", peer)
            peer.close()
        else:
            return [peer]

    reader_writer_iterator = flatten_aiter(parallel_map_aiter(
        host_port_to_reader_writer, tcp_connect_workers, host_iterator))

    connected_peer_iterator = flatten_aiter(parallel_map_aiter(
        peer_handshake, handshake_workers, reader_writer_iterator))

    return connected_peer_iterator


def get_peer_iterator(network, peer_addresses=None):
    # for now, let's just do one peer
    host_iterator = None
    if peer_addresses:
        for peer in peer_addresses:
            if "/" in peer:
                host, port = peer.split("/", 1)
                port = int(port)
            else:
                host = peer
                port = network.default_port
            host_iterator = iter_to_aiter([(host, port)])
    # BRAIN DAMAGE: 70016 version number is required for bgold new block header format
    return peer_connect_iterator(network, host_iterator=host_iterator, version_dict=dict(version=70016))


# events
async def perform_handshake(peer, **version_msg):
    """
    Call this method to kick of event processing.
    """
    # "version"
    peer.send_msg("version", **version_msg)
    event = await peer.next_message()
    if event is None:
        return None
    msg, version_data = event
    if msg != 'version':
        return None

    # "verack"
    peer.send_msg("verack")
    event = await peer.next_message()
    if event is None:
        return None
    msg, verack_data = event
    if msg != 'verack':
        return None

    return version_data
