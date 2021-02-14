import asyncio
import logging

from aiter import parallel_map_aiter, flatten_aiter

from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer


async def host_port_to_reader_writer(host_port_pair):
    host, port = host_port_pair
    logging.debug("TCP connecting to %s:%d", host, port)
    try:
        reader, writer = await asyncio.open_connection(host=host, port=port)
        logging.debug("TCP connected to %s:%d", host, port)
        return [(reader, writer)]
    except Exception as ex:
        logging.info("connect failed: %s:%d (%s)", host, port, ex)


def make_peer_handshake_map_filter(network, version_dict):
    async def peer_handshake(reader_writer):
        reader, writer = reader_writer
        peer = Peer(
            reader, writer, network.magic_header, network.message.parse,
            network.message.pack, max_msg_size=10*1024*1024)
        version_data = version_data_for_peer(peer, **version_dict)
        peer.version = await perform_handshake(peer, **version_data)
        if peer.version is None:
            logging.info("handshake failed on %s", peer)
            peer.close()
        else:
            return [peer]
    return peer_handshake


def make_handshaked_peer_aiter(
        network, host_aiter, tcp_connect_workers=30, handshake_workers=3, version_dict={}):
    """
    host_aiter: async iter of (host, port) values
    connected_host_aiter: async iter of (reader, writer) values
    handshaked_peer_aiter: async iter of Peer objects, post handshake
    """

    peer_handshake_map_filter = make_peer_handshake_map_filter(network, version_dict)

    connected_host_aiter = flatten_aiter(parallel_map_aiter(
        host_port_to_reader_writer, tcp_connect_workers, host_aiter))

    handshaked_peer_aiter = flatten_aiter(
        parallel_map_aiter(peer_handshake_map_filter, handshake_workers, connected_host_aiter))

    return handshaked_peer_aiter


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
