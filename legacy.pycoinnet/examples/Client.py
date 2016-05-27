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

from pycoin.blockchain.BlockChain import BlockChain

from pycoinnet.InvItem import ITEM_TYPE_BLOCK

from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol
from pycoinnet.peer.Fetcher import Fetcher

from pycoinnet.peergroup.fast_forwarder import fast_forwarder_add_peer_f
from pycoinnet.peergroup.Blockfetcher import Blockfetcher
from pycoinnet.peergroup.BlockHandler import BlockHandler
from pycoinnet.peergroup.InvCollector import InvCollector

from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import install_pingpong_manager
from pycoinnet.helpers.standards import manage_connection_count
from pycoinnet.helpers.standards import version_data_for_peer

from pycoinnet.util.TwoLevelDict import TwoLevelDict


def block_chain_locker_callback(block_chain, ops):
    LOCKED_MULTIPLE = 32
    total_length = block_chain.length()
    locked_length = block_chain.locked_length()
    unlocked_length = total_length - locked_length
    if unlocked_length > LOCKED_MULTIPLE:
        new_locked_length = total_length - (total_length % LOCKED_MULTIPLE) - LOCKED_MULTIPLE
        block_chain.lock_to_index(new_locked_length)


@asyncio.coroutine
def new_block_fetcher(inv_collector, block_chain):
    item_q = inv_collector.new_inv_item_queue()
    while True:
        inv_item = yield from item_q.get()
        if inv_item.item_type == ITEM_TYPE_BLOCK:
            if not block_chain.is_hash_known(inv_item.data):
                block = yield from inv_collector.fetch(inv_item)
                block_chain.add_headers([block])


@asyncio.coroutine
def show_connection_info(connection_info_q):
    while True:
        verb, noun, peer = yield from connection_info_q.get()
        logging.info("connection manager: %s on %s", verb, noun)


class Client(object):

    def __init__(self, network, host_port_q, should_download_block_f, block_chain_store,
                 blockchain_change_callback, server_port=9999):
        """
        network:
            a value from pycoinnet.helpers.networks
        host_port_q:
            a Queue that is being fed potential places to connect
        should_download_block_f:
            a function that accepting(block_hash, block_index) and returning a boolean
            indicating whether that block should be downloaded. Only used during fast-forward.
        block_chain_store:
            usually a BlockChainStore instance
        blockchain_change_callback:
            a callback that expects (blockchain, list_of_ops) that is invoked whenever the
            block chain is updated; blockchain is a BlockChain object and list_of_ops is a pair
            of tuples of the form (op, block_hash, block_index) where op is one of "add" or "remove",
            block_hash is a binary block hash, and block_index is an integer index number.
        """

        block_chain = BlockChain(did_lock_to_index_f=block_chain_store.did_lock_to_index)

        block_chain.preload_locked_blocks(block_chain_store.headers())

        block_chain.add_change_callback(block_chain_locker_callback)

        self.blockfetcher = Blockfetcher()
        self.inv_collector = InvCollector()

        self.block_store = TwoLevelDict()

        @asyncio.coroutine
        def _rotate(block_store):
            while True:
                block_store.rotate()
                yield from asyncio.sleep(1800)
        self.rotate_task = asyncio.Task(_rotate(self.block_store))

        self.blockhandler = BlockHandler(self.inv_collector, block_chain, self.block_store,
                                         should_download_f=should_download_block_f)

        block_chain.add_change_callback(blockchain_change_callback)

        self.fast_forward_add_peer = fast_forwarder_add_peer_f(block_chain)
        self.fetcher_task = asyncio.Task(new_block_fetcher(self.inv_collector, block_chain))

        self.nonce = int.from_bytes(os.urandom(8), byteorder="big")
        self.subversion = "/Notoshi/".encode("utf8")

        @asyncio.coroutine
        def run_peer(peer, fetcher, fast_forward_add_peer, blockfetcher, inv_collector, blockhandler):
            yield from asyncio.wait_for(peer.connection_made_future, timeout=None)
            version_parameters = version_data_for_peer(
                peer, local_port=(server_port or 0), last_block_index=block_chain.length(),
                nonce=self.nonce, subversion=self.subversion)
            version_data = yield from initial_handshake(peer, version_parameters)
            last_block_index = version_data["last_block_index"]
            fast_forward_add_peer(peer, last_block_index)
            blockfetcher.add_peer(peer, fetcher, last_block_index)
            inv_collector.add_peer(peer)
            blockhandler.add_peer(peer)

        def create_protocol_callback():
            peer = BitcoinPeerProtocol(network["MAGIC_HEADER"])
            install_pingpong_manager(peer)
            fetcher = Fetcher(peer)
            peer.add_task(run_peer(
                peer, fetcher, self.fast_forward_add_peer,
                self.blockfetcher, self.inv_collector, self.blockhandler))
            return peer

        self.connection_info_q = manage_connection_count(host_port_q, create_protocol_callback, 8)
        self.show_task = asyncio.Task(show_connection_info(self.connection_info_q))

        # listener
        @asyncio.coroutine
        def run_listener():
            abstract_server = None
            try:
                abstract_server = yield from asyncio.get_event_loop().create_server(
                    protocol_factory=create_protocol_callback, port=server_port)
                return abstract_server
            except OSError:
                logging.info("can't listen on port %d", server_port)

        if server_port:
            self.server_task = asyncio.Task(run_listener())

    def add_blocks(self, blocks):
        for block in blocks:
            self.blockhandler.add_block(block)
        self.blockhandler.block_chain.add_headers(blocks)

    def add_block(self, block):
        self.add_blocks([block])

    def blockchain_length(self):
        return self.blockhandler.block_chain.length()

    def add_got_header_callback(self, callback):
        pass
