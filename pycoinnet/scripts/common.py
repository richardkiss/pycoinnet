#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import asyncio
import logging
import os.path

from pycoin.message.PeerAddress import PeerAddress

from pycoinnet.BlockChainView import BlockChainView


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


VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, bytes([127, 0, 0, 2]), 6111),
    local_address=PeerAddress(1, bytes([127, 0, 0, 1]), 6111),
    nonce=3412075413544046060,
    last_block_index=10000
)


def install_pong_manager(peer):
    def handle_msg(name, data):
        if name == 'ping':
            peer.send_msg("pong", nonce=data["nonce"])
    peer.add_msg_handler(handle_msg)
