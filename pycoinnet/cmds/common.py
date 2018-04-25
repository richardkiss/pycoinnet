"""
This tool gets all headers quickly and prints summary of chain state.
"""

import asyncio
import logging
import os.path

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


def set_log_file(logPath, level=logging.NOTSET):
    if logPath is None:
        return
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)


def storage_base_path():
    p = os.path.expanduser("~/.pycoinnet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


def parser_and_packer_for_network(network):
    # BRAIN DAMAGE
    from pycoin.message.make_parser_and_packer import (
        make_parser_and_packer, standard_messages,
        standard_message_post_unpacks, standard_streamer, standard_parsing_functions
    )

    streamer = standard_streamer(standard_parsing_functions(network.block, network.tx))
    return make_parser_and_packer(streamer, standard_messages(), standard_message_post_unpacks(streamer))


def peer_connect_pipeline(network, tcp_connect_workers=30, handshake_workers=3, host_q=None, loop=None):

    host_q = host_q or dns_bootstrap_host_port_q(network)

    async def do_tcp_connect(host_port_pair, q):
        host, port = host_port_pair
        logging.debug("TCP connecting to %s:%d", host, port)
        reader, writer = await asyncio.open_connection(host=host, port=port)
        logging.debug("TCP connected to %s:%d", host, port)
        await q.put((reader, writer))

    async def do_peer_handshake(rw_tuple, q):
        reader, writer = rw_tuple
        parse_from_data, pack_from_data = parser_and_packer_for_network(network)
        peer = Peer(reader, writer, network.magic_header, parse_from_data, pack_from_data)
        version_data = version_data_for_peer(peer, **version_dict)
        peer.version = await peer.perform_handshake(**version_data)
        await q.put(peer)

    filters = [
        dict(callback_f=do_tcp_connect, input_q=host_q, worker_count=tcp_connect_workers),
        dict(callback_f=do_peer_handshake, worker_count=handshake_workers),
    ]
    return MappingQueue(*filters, loop=loop)


def get_current_view(path):
    try:
        with open(path) as f:
            return BlockChainView.from_json(f.read())
    except FileNotFoundError:
        pass
    return BlockChainView()


def save_bcv(path, bcv):
    json = bcv.as_json(sort_keys=True, indent=2)
    tmp = "%s.tmp" % path
    with open(tmp, "w") as f:
        f.write(json)
    os.rename(tmp, path)
