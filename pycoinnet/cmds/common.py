"""
This tool gets all headers quickly and prints summary of chain state.
"""

import asyncio
import logging
import os.path

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer


def init_logging():
    LOG_FORMAT = ('%(asctime)s [%(process)d] [%(levelname)s] '
                  '%(filename)s:%(lineno)d %(message)s')

    asyncio.tasks._DEBUG = True
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.INFO)


def storage_base_path():
    p = os.path.expanduser("~/.pycoinnet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


async def connect_peer(network, host, port):
    reader, writer = await asyncio.open_connection(host=host, port=port)
    logging.info("connecting to %s:%d", host, port)
    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    version_data = version_data_for_peer(peer)
    await peer.perform_handshake(**version_data)
    peer.start_dispatcher()
    return peer


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


def install_pong_manager(peer):
    def handle_msg(name, data):
        if name == 'ping':
            peer.send_msg("pong", nonce=data["nonce"])
    peer.add_msg_handler(handle_msg)
