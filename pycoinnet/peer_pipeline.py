import asyncio
import logging

from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer

from pycoin.message.make_parser_and_packer import (
    make_parser_and_packer, standard_messages,
    standard_message_post_unpacks, standard_streamer, standard_parsing_functions
)


def create_hostport_to_peers_q(
        network, peer_count=8, output_q=None, version_dict={}, connect_workers=30,
        connect_callback=None, loop=None):

    async def do_peer_connect(host_port_pair, q):
        host, port = host_port_pair
        logging.debug("TCP connecting to %s:%d", host, port)
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            logging.debug("TCP connected to %s:%d", host, port)
            streamer = standard_streamer(standard_parsing_functions(network.block, network.tx))
            parse_from_data, pack_from_data = make_parser_and_packer(
                streamer, standard_messages(), standard_message_post_unpacks(streamer))
            peer = Peer(
                reader, writer, network.magic_header, parse_from_data,
                pack_from_data, max_msg_size=10*1024*1024)
            version_data = version_data_for_peer(peer, **version_dict)
            peer.version = await peer.perform_handshake(**version_data)
            if peer.version is None:
                logging.info("handshake failed on %s", peer)
                peer.close()
                return
            await q.put(peer)
        except Exception as ex:
            logging.info("connect failed: %s:%d (%s)", host, port, ex)

    async def wait_until_peer_done(peer, q):
        if connect_callback:
            await connect_callback(peer)
        await q.put(peer)
        peer.start()
        await peer.wait_until_close()

    filters = [
        dict(callback_f=do_peer_connect, worker_count=connect_workers),
        dict(callback_f=wait_until_peer_done, worker_count=peer_count),
    ]

    return MappingQueue(*filters, final_q=output_q, loop=loop)
