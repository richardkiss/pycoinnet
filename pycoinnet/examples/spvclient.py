#!/usr/bin/env python

"""
A prototype of a custom bitcoin client with pluggables for callbacks.

PARAMETERS:
    - network (MAINNET)
    - callback for blockchain change
    - callback for Tx received
    - callback to validate Tx
"""

import asyncio
import os
import logging

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.Fetcher import Fetcher

from pycoinnet.peergroup.getheaders import getheaders_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.helpers.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_pingpong_manager
from pycoinnet.helpers.standards import manage_connection_count
from pycoinnet.helpers.standards import version_data_for_peer


@asyncio.coroutine
def show_connection_info(connection_info_q):
    while True:
        verb, noun, peer = yield from connection_info_q.get()
        logging.info("connection manager: %s on %s", verb, noun)


class SPVClient(object):
    """
    add_tracked_address
    add_tracked_spendable
    (etc.)

    add_blockchain_view_delta_callback
    add_got_tx_callback

    merkleblocks_for_headers
    """

    def __init__(self, network, initial_blockchain_view, bloom_filter, merkle_block_index_queue,
                 host_port_q=None):
        """
        network:
            a value from pycoinnet.helpers.networks
        initial_blockchain_view:
            BlockChainView instance which is update automatically
        bloom_filter:
            the filter sent to remotes
        merkle_block_index_queue:
            a Queue which is fed with (merkleblock, index) items which need to be processed
        host_port_q:
            a Queue that is being fed potential places to connect
        """

        if host_port_q is None:
            host_port_q = dns_bootstrap_host_port_q(network)

        self.network = network
        self.blockchain_view = initial_blockchain_view
        self.bloom_filter = bloom_filter

        self.merkle_block_futures = asyncio.Queue(maxsize=2000)
        self.feed_task = asyncio.Task(self.feed_merkle_blocks(merkle_block_index_queue))

        self.blockfetcher = Blockfetcher()
        self.inv_collector = InvCollector()

        self.getheaders_add_peer = getheaders_add_peer_f(self.blockchain_view, self.handle_reorg)

        self.nonce = int.from_bytes(os.urandom(8), byteorder="big")
        self.subversion = "/Notoshi/".encode("utf8")

        @asyncio.coroutine
        def run_peer(peer, fetcher, getheaders_add_peer, blockfetcher, inv_collector):
            yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
            last_block_index = max(0, self.blockchain_view.last_block_index())
            version_parameters = version_data_for_peer(
                peer, local_port=0, last_block_index=last_block_index, nonce=self.nonce,
                subversion=self.subversion)
            version_data = yield from initial_handshake(peer, version_parameters)
            filter_bytes, hash_function_count, tweak = self.bloom_filter.filter_load_params()
            # TODO: figure out flags
            flags = 0
            peer.send_msg(
                "filterload", filter=filter_bytes, hash_function_count=hash_function_count,
                tweak=tweak, flags=flags)
            last_block_index = version_data["last_block_index"]
            getheaders_add_peer(peer, last_block_index)
            blockfetcher.add_peer(peer, fetcher, last_block_index)
            inv_collector.add_peer(peer)

        def create_protocol_callback():
            peer = BitcoinPeerProtocol(network["MAGIC_HEADER"])
            install_pingpong_manager(peer)
            fetcher = Fetcher(peer)
            peer.add_task(run_peer(
                peer, fetcher, self.getheaders_add_peer,
                self.blockfetcher, self.inv_collector))
            return peer

        self.connection_info_q = manage_connection_count(host_port_q, create_protocol_callback, 8)
        self.show_task = asyncio.Task(show_connection_info(self.connection_info_q))

    def merkleblock_futures_for_headers(self, block_number, headers):
        return [self.blockfetcher.get_merkle_block_future(h, idx) for idx, h in enumerate(headers)]

    @asyncio.coroutine
    def feed_merkle_blocks(self, merkle_block_index_queue):
        while 1:
            index, future = yield from self.merkle_block_futures.get()
            merkle_block = yield from future
            yield from merkle_block_index_queue.put([merkle_block, index])

    @asyncio.coroutine
    def handle_reorg(self, block_number, headers):
        for idx, h in enumerate(headers):
            yield from self.merkle_block_futures.put(
                [block_number+idx, self.blockfetcher.get_merkle_block_future(h.hash(), block_number+idx)])


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))

    from pycoinnet.helpers.networks import MAINNET
    from pycoinnet.util.BlockChainView import BlockChainView
    from pycoinnet.bloom import BloomFilter
    from pycoin.tx import Spendable
    from pycoin.serialize import h2b_rev, h2b
    network = MAINNET
    initial_blockchain_view = BlockChainView()
    bloom_filter = BloomFilter(2048, hash_function_count=8, tweak=3)
    bloom_filter.add_address("14gZfnEn8Xd3ofkjr5s7rKoC3bi8J4Yfyy")
    # bloom_filter.add_address("1GL6i1ty44RnERgqYLKS1CrnhrahW4JhQZ")
    bloom_filter.add_item(h2b("0478abb18c0c7c95348fa77eb5fd43ce963e450d797cf4878894230ca528e6c8e866c3"
                              "8ad93746e04f2161a01787c82a858ee24940e9a06e41fddb3494dfe29380"))
    spendable = Spendable(
        0, b'', h2b_rev("0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9"), 0)
    bloom_filter.add_spendable(spendable)
    merkle_block_index_queue = asyncio.Queue()
    spv = SPVClient(
        network, initial_blockchain_view, bloom_filter, merkle_block_index_queue, host_port_q=None)

    def fetch(merkle_block_index_queue):
        while True:
            merkle_block, index = yield from merkle_block_index_queue.get()
            logging.info(
                "block #%d %s with %d transactions", index, merkle_block.id(), len(merkle_block.txs))

    t = asyncio.Task(fetch(merkle_block_index_queue))

    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
