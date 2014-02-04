
"""
Keep track of all connected peers.
"""

import asyncio.queues
import binascii
import logging

from pycoin.serialize import b2h_rev

from pycoinnet.util.Queue import Queue

logging = logging.getLogger("ConnectionManager")

class ConnectionManager:

    def __init__(self, address_queue, peer_protocol_factory, min_connection_count=4, max_connection_count=10):
        self.address_queue = address_queue
        self.peers_connecting = set()
        self.peers_connected = set()
        self.peer_protocol_factory = peer_protocol_factory
        self.min_connection_count = min_connection_count
        self.max_connection_count = max_connection_count

    def handle_connection_made(self, peer, transport):
        logging.debug("connection made %s", transport)
        self.peers_connected.add(peer)

    def handle_connection_lost(self, peer, exc):
        self.peers_connected.remove(peer)

    def run(self):
        asyncio.Task(self.manage_connection_count())

    @asyncio.coroutine
    def manage_connection_count(self):
        while 1:
            count = len(self.peers_connected)
            logging.debug("checking connection count (currently %d)", count)
            if count < self.min_connection_count:
                difference = self.min_connection_count - len(self.peers_connected)
                for i in range(difference*3):
                    host, port = yield from self.address_queue.get()
                    asyncio.Task(self.connect_to_remote(host, port))
            if count > self.max_connection_count:
                # pick a peer at random and disconnect
                peer = next(iter(self.peers_connected))
                peer.stop()
            yield from asyncio.sleep(10)

    @asyncio.coroutine
    def connect_to_remote(self, host, port):
        logging.info("connecting to %s port %d", host, port)
        try:
            peer_name = "%s:%d" % (host, port)
            self.peers_connecting.add(peer_name)
            transport, protocol = yield from asyncio.get_event_loop().create_connection(
                self.peer_protocol_factory, host=host, port=port)
            logging.info("connected (tcp) to %s:%d", host, port)
        except Exception:
            logging.exception("failed to connect to %s:%d", host, port)
        self.peers_connecting.remove(peer_name)
