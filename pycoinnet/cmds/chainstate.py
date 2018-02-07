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
from pycoinnet.MappingQueue import MappingQueue

from pycoinnet.cmds.common import (
    init_logging, connect_peer, storage_base_path, get_current_view, save_bcv
)


async def update_chain_state(network, bcv, count=3):

    update_q = asyncio.Queue()

    dns_q = dns_bootstrap_host_port_q(network)

    async def do_connect_peer(item, q):
        host, port = item
        peer = await connect_peer(network, host, port)
        await q.put((peer, item))

    async def do_improve_headers(pair, q):
        peer, item = pair
        r = await improve_headers(peer, bcv, update_q)
        peer.close()
        await peer.wait_for_cleanup()
        await q.put(item)

    filters = [
        dict(callback_f=do_connect_peer, input_q=dns_q, worker_count=30),
        dict(callback_f=do_improve_headers, worker_count=3),
    ]
    q = MappingQueue(*filters)

    for _ in range(count):
        v = await q.get()
        print(v)


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

    asyncio.get_event_loop().run_until_complete(update_chain_state(network, bcv))
    bcv.winnow()
    save_bcv(path, bcv)
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
