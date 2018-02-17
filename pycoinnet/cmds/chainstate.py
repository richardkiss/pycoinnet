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
    init_logging, set_log_file, peer_connect_pipeline, storage_base_path, get_current_view, save_bcv, install_pong_manager
)


async def update_chain_state(network, bcv, count=3):

    peer_q = peer_connect_pipeline(network)

    update_q = asyncio.Queue()

    async def do_improve_headers(peer, q):
        peer.start_dispatcher()
        install_pong_manager(peer)
        r = await improve_headers(peer, bcv, update_q)
        await q.put(peer)

    filters = [
        dict(callback_f=do_improve_headers, input_q=peer_q, worker_count=count),
    ]
    q = MappingQueue(*filters)

    for _ in range(count):
        peer = await q.get()
        print("finished update from %s" % peer)
        peer.close()
        await peer.wait_for_cleanup()
    await q.cancel()
    asyncio.get_event_loop().stop()


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')
    parser.add_argument('-l', "--log-file", help="Path to log file", default=None)

    args = parser.parse_args()

    set_log_file(args.log_file)

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
