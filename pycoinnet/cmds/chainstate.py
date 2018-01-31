#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path

from pycoin.serialize import b2h_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.headerpipeline import improve_headers
from pycoinnet.networks import MAINNET

from pycoinnet.cmds.common import (
    init_logging, connect_peer, storage_base_path, get_current_view, save_bcv
)


async def monitor_update_q(network, path, bcv, count):
    update_q = asyncio.Queue()
    ucs = asyncio.get_event_loop().create_task(update_chain_state(network, bcv, update_q, count))
    while True:
        v = await update_q.get()
        if v is None:
            break
        bcv.winnow()
        save_bcv(path, bcv)
    await ucs


@asyncio.coroutine
def update_current_view(network, host, port, bcv, peer_set, update_q):
    peer = yield from connect_peer(network, host, port)
    peer_set.add(peer)
    yield from improve_headers(peer, bcv, update_q)


async def update_chain_state(network, bcv, update_q, count=3):
    peers = set()
    futures = []
    q = dns_bootstrap_host_port_q(network)
    for _ in range(count):
        peer_addr = await q.get()
        if peer_addr is None:
            break
        host, port = peer_addr
        futures.append(update_current_view(network, host, port, bcv, peers, update_q))
        if len(futures) >= count:
            break
    await asyncio.wait(futures)
    update_q.put_nowait(None)
    for peer in peers:
        peer.close()
    for peer in peers:
        await peer.wait_for_cleanup()


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    bcv = get_current_view(path)
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))

    network = MAINNET

    r = asyncio.get_event_loop().run_until_complete(monitor_update_q(network, path, bcv, count=3))
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
