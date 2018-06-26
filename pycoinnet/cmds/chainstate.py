#!/usr/bin/env python

"""
This tool gets all headers quickly and prints summary of chain state.
"""

import argparse
import asyncio
import logging
import os.path
import time

from pycoin.encoding.hexbytes import b2h_rev
from pycoin.networks.registry import network_for_netcode

from pycoinnet.blockcatchup import create_peer_to_header_q
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.cmds.common import (
    init_logging, set_log_file, storage_base_path, get_current_view, save_bcv
)
from pycoinnet.peer_pipeline import create_hostport_to_peers_q
from pycoinnet.pong_manager import install_pong_manager


def main():
    init_logging(level=logging.DEBUG, asyncio_debug=True)
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('-p', "--path", help='The path to the wallet files.')
    parser.add_argument('-l', "--log-file", help="Path to log file", default=None)

    args = parser.parse_args()

    set_log_file(args.log_file)

    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    bcv = get_current_view(path)
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))

    network = args.network

    inv_batcher = InvBatcher()

    async def peer_callback(peer):
        install_pong_manager(peer)
        await inv_batcher.add_peer(peer)

    peer_header_q = create_peer_to_header_q(bcv.node_tuples, inv_batcher)
    hostport_to_peers_q = create_hostport_to_peers_q(
        network, peer_count=3, output_q=peer_header_q, connect_callback=peer_callback)
    dns_bootstrap_task = dns_bootstrap_host_port_q(network, output_q=hostport_to_peers_q)

    last_save = time.time()
    while True:
        r = asyncio.get_event_loop().run_until_complete(peer_header_q.get())
        if r is None:
            break
        block_index, headers = r
        block_number = bcv.do_headers_improve_path(headers)
        now = time.time()
        if now > last_save + 30:
            save_bcv(path, bcv)
            last_save = now
    dns_bootstrap_task.stop()
    save_bcv(path, bcv)
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
