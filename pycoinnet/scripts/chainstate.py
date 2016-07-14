#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.message.PeerAddress import PeerAddress
from pycoin.serialize import b2h_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.headerpipeline import improve_headers
from pycoinnet.networks import MAINNET, TESTNET

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.Peer import Peer


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


@asyncio.coroutine
def connect_peer(network, host, port):
    reader, writer = yield from asyncio.open_connection(host=host, port=port)
    logging.info("connecting to %s:%d", host, port)
    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    yield from peer.perform_handshake(**VERSION_MSG)
    peer.start_dispatcher()
    return peer


@asyncio.coroutine
def monitor_update_q(path, bcv, update_q):
    while True:
        v = yield from update_q.get()
        if v is None:
            break
        bcv.winnow()
        save_bcv(path, bcv)


@asyncio.coroutine
def update_current_view(network, host, port, bcv, peer_set, update_q):
    peer = yield from connect_peer(network, host, port)
    peer_set.add(peer)
    yield from improve_headers(peer, bcv, update_q)


@asyncio.coroutine
def update_chain_state(network, bcv, update_q, peer_set, count=3):
    futures = []
    q = dns_bootstrap_host_port_q(network)
    for _ in range(count):
        peer_addr = yield from q.get()
        if peer_addr is None:
            break
        host, port = peer_addr
        futures.append(update_current_view(network, host, port, bcv, peer_set, update_q))
        if len(futures) >= count:
            break
    yield from asyncio.wait(futures)


def main():
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    loop = asyncio.get_event_loop()
    update_q = asyncio.Queue()
    bcv = get_current_view(path)
    peers = set()
    muq_task = loop.create_task(monitor_update_q(path, bcv, update_q))
    loop.run_until_complete(update_chain_state(MAINNET, bcv, update_q, peers))
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    for peer in peers:
        peer.close()
    for peer in peers:
        loop.run_until_complete(peer.wait_for_cleanup())
    update_q.put_nowait(None)
    loop.run_until_complete(muq_task)
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
