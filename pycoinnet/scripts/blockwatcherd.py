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
from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.msg.PeerAddress import PeerAddress
from pycoinnet.networks import MAINNET, TESTNET

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.BlockChainView import BlockChainView, HASH_INITIAL_BLOCK
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
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=10000
)


@asyncio.coroutine
def _fetch_missing(peer, header):
    the_hash = header.previous_block_hash
    inv_item = InvItem(ITEM_TYPE_BLOCK, the_hash)
    logging.info("requesting missing block header %s", inv_item)
    peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, the_hash)])
    name, data = yield from peer.wait_for_response('block')
    block = data["block"]
    logging.info("got missing block %s", block.id())
    return block


@asyncio.coroutine
def do_get_headers(peer, block_locator_hashes, hash_stop=b'\0'*32):
    peer.send_msg(message_name="getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
    name, data = yield from peer.wait_for_response('headers')
    headers = [bh for bh, t in data["headers"]]
    return headers


@asyncio.coroutine
def update_headers(network, q, bcv, update_q, peer_created_callback):
    while 1:
        peer_addr = yield from q.get()
        if peer_addr is None:
            return
        host, port = peer_addr
        logging.info("connecting to %s:%d", host, port)
        reader, writer = yield from asyncio.open_connection(host=host, port=port)
        break

    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    yield from peer.perform_handshake(**VERSION_MSG)
    peer_created_callback(peer)
    peer.start_dispatcher()

    while True:
        block_locator_hashes = bcv.block_locator_hashes()
        headers = yield from do_get_headers(peer, bcv.block_locator_hashes())
        if block_locator_hashes[-1] == HASH_INITIAL_BLOCK:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            extra_block = yield from _fetch_missing(peer, headers[0])
            headers = [extra_block] + headers

        if len(headers) == 0:
            break
        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            continue
        logging.debug("block header count is now %d", block_number)
        hashes = []
        for idx in range(block_number, bcv.last_block_index()+1):
            the_tuple = bcv.tuple_for_index(idx)
            assert the_tuple[0] == idx
            hashes.append(the_tuple[1])
        update_q.put_nowait((block_number, hashes))
        bcv.winnow()


@asyncio.coroutine
def update_headers_pipeline(network, bcv, count, update_q, peer_created_callback):
    futures = []
    q = dns_bootstrap_host_port_q(network)
    bcv_copy = bcv.clone()
    for _ in range(count):
        futures.append(
            update_headers(network, q, bcv_copy, update_q, peer_created_callback))
        if len(futures) >= count:
            break
    yield from asyncio.wait(futures)
    update_q.put_nowait(None)


@asyncio.coroutine
def handle_update_q(bcv, path, block_fetcher, update_q):
    MAX_BATCH_SIZE = 32
    block_update = []
    loop = asyncio.get_event_loop()
    while 1:
        v = yield from update_q.get()
        if v is None:
            break
        first_block_index, block_hashes = v
        print("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]
        block_futures = block_fetcher.fetch_blocks(block_hash_priority_pair_list)
        for _, bf in enumerate(block_futures):
            block = yield from bf
            block_index = first_block_index + _
            block_update.append((block_index, block))
            if len(block_update) >= MAX_BATCH_SIZE:
                yield from loop.run_in_executor(None, flush_block_update, bcv, path, block_update)
    yield from loop.run_in_executor(None, flush_block_update, bcv, path, block_update)


def flush_block_update(bcv, path, block_update):
    block_index, block = block_update[0]
    logging.info("updating %d blocks starting at %d for path %s" % (len(block_update), block_index, path))
    block_number = bcv.do_headers_improve_path([block for _, block in block_update])
    if block_number is not False:
        bcv.winnow()
        save_bcv(path, bcv)
    block_update[:] = []


def main():
    parser = argparse.ArgumentParser(description="Update chain state and print summary.")
    parser.add_argument('-p', "--path", help='The path to the wallet files.')

    args = parser.parse_args()
    path = os.path.join(args.path or storage_base_path(), "chainstate.json")

    block_fetcher = Blockfetcher()
    loop = asyncio.get_event_loop()
    bcv = get_current_view(path)
    network = TESTNET

    update_q = asyncio.Queue()
    update_q_task = loop.create_task(handle_update_q(bcv, path, block_fetcher, update_q))
    loop.run_until_complete(update_headers_pipeline(
        network, bcv, count=3, update_q=update_q, peer_created_callback=block_fetcher.add_peer))
    last_index, last_block_hash, total_work = bcv.last_block_tuple()
    loop.run_until_complete(update_q_task)
    print("last block index %d, hash %s" % (last_index, b2h_rev(last_block_hash)))


if __name__ == '__main__':
    main()
