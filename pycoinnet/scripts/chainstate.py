#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.serialize import b2h_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_lists
from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.msg.PeerAddress import PeerAddress
from pycoinnet.networks import MAINNET

from pycoinnet.BlockChainView import BlockChainView, HASH_INITIAL_BLOCK
from pycoinnet.Dispatcher import Dispatcher
from pycoinnet.PeerProtocol import PeerProtocol


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
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=10000
)


@asyncio.coroutine
def _fetch_missing(peer, dispatcher, header):
    the_hash = header.previous_block_hash
    inv_item = InvItem(ITEM_TYPE_BLOCK, the_hash)
    logging.info("requesting missing block header %s", inv_item)
    peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, the_hash)])
    name, data = yield from dispatcher.wait_for_response('block')
    block = data["block"]
    logging.info("got missing block %s", block.id())
    return block


@asyncio.coroutine
def do_get_headers(peer, dispatcher, block_locator_hashes, hash_stop=b'\0'*32):
    peer.send_msg(message_name="getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
    name, data = yield from dispatcher.wait_for_response('headers')
    headers = [bh for bh, t in data["headers"]]
    return headers


@asyncio.coroutine
def update_current_view(network, host, port, bcv, path):
    loop = asyncio.get_event_loop()
    transport, peer = yield from loop.create_connection(
        lambda: PeerProtocol(network), host=host, port=port)
    logging.info("connecting to %s:%d", host, port)
    dispatcher = Dispatcher(peer)
    yield from dispatcher.handshake(**VERSION_MSG)
    dispatcher.start()

    while True:
        block_locator_hashes = bcv.block_locator_hashes()
        headers = yield from do_get_headers(peer, dispatcher, bcv.block_locator_hashes())
        if block_locator_hashes[-1] == HASH_INITIAL_BLOCK:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            extra_block = yield from _fetch_missing(peer, dispatcher, headers[0])
            headers = [extra_block] + headers

        if len(headers) == 0:
            dispatcher.stop()
            return
        block_number = bcv.do_headers_improve_path(headers)
        if block_number is not False:
            logging.debug("block header count is now %d", block_number)
        bcv.winnow()
        save_bcv(path, bcv)


@asyncio.coroutine
def create_update_futures(network, path, bcv, count):
    futures = []
    for f in dns_bootstrap_host_port_lists(network, count=count):
        peer_addr = yield from f
        if peer_addr is None:
            return futures
        host, port = peer_addr
        futures.append(update_current_view(network, host, port, bcv, path))
        if len(futures) >= count:
            return futures


@asyncio.coroutine
def update_chain_state(network, path, count=3):
    bcv = get_current_view(path)
    futures = yield from create_update_futures(network, path, bcv, count)
    yield from asyncio.wait(futures)
    return bcv


def main():
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    loop = asyncio.get_event_loop()
    bcv = loop.run_until_complete(update_chain_state(MAINNET, path))
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
