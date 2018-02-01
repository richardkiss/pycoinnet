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
from pycoinnet.pipeline import pipeline

from pycoinnet.cmds.common import (
    init_logging, connect_peer, storage_base_path, get_current_view, save_bcv
)


async def zimprove_headers(*args):
    # BRAIN DAMAGE
    await asyncio.sleep(5)


async def update_chain_state(network, bcv, update_q, count=3):
    futures = []
    loop = asyncio.get_event_loop()

    address_q = dns_bootstrap_host_port_q(network)

    futures = []
    peer_set = set()

    got_enough = loop.create_future()

    async def do_connect_peer(item):
        host, port = item
        peer = await connect_peer(network, host, port)
        if len(peer_set) < count:
            peer_set.add(peer)
            future = asyncio.ensure_future(improve_headers(peer, bcv, update_q))
            futures.append(future)
            await future
        if len(peer_set) >= count:
            if not got_enough.done():
                got_enough.set_result(None)
            peer.close()
            logging.info("closing unnecessary peer %s", peer)
            address_q.close(cancel_pending=False)

    pipeline(do_connect_peer, q=address_q)

    await got_enough
    for f in futures:
        await f
    update_q.put_nowait(None)
    import pdb; pdb.set_trace()
    for peer in peer_set:
        peer.close()
    for peer in peer_set:
        await peer.wait_for_cleanup()


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

    asyncio.get_event_loop().run_until_complete(monitor_update_q(network, path, bcv, count=3))
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
