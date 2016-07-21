#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path
import sys

from pycoin.serialize import b2h_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.headerpipeline import improve_headers
from pycoinnet.networks import MAINNET, TESTNET

from pycoinnet.Peer import Peer

from pycoinnet.scripts.common import (
    init_logging, storage_base_path, get_current_view, save_bcv, VERSION_MSG
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
    init_logging()
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    loop = asyncio.get_event_loop()
    update_q = asyncio.Queue()
    bcv = get_current_view(path)
    network = MAINNET
    peers = set()

    muq_task = loop.create_task(monitor_update_q(path, bcv, update_q))
    loop.run_until_complete(update_chain_state(network, bcv, update_q, peers))
    for peer in peers:
        peer.close()
    for peer in peers:
        loop.run_until_complete(peer.wait_for_cleanup())
    update_q.put_nowait(None)
    loop.run_until_complete(muq_task)
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
