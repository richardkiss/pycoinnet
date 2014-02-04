
import asyncio
import logging
import os
import struct
import time

logging = logging.getLogger("PingPongHandler")

class PingPongHandler:
    def __init__(self, peer, heartbeat_rate=60, missing_pong_disconnect_timeout=60):
        self.peer = peer
        self.ping_nonces = set()
        self.heartbeat_rate = heartbeat_rate
        self.missing_pong_disconnect_timeout = missing_pong_disconnect_timeout
        self.last_message_timestamp = time.time()

        peer.register_delegate(self)

    def handle_connection_made(self, peer, transport):
        self.ping_task = asyncio.Task(self.run())

    def handle_connection_lost(self, peer, exc):
        self.ping_task.cancel()

    def run(self):
        while True:
            now = time.time()
            if self.last_message_timestamp + self.heartbeat_rate < now:
                # we need to ping!
                nonce = struct.unpack("!Q", os.urandom(8))[0]
                self.peer.send_msg("ping", nonce=nonce)
                logging.debug("sending ping %d", nonce)
                self.ping_nonces.add(nonce)
                yield from asyncio.sleep(self.missing_pong_disconnect_timeout)
                if nonce in self.ping_nonces:
                    # gotta hang up!
                    self.peer.stop()
            yield from asyncio.sleep(self.last_message_timestamp + self.heartbeat_rate - now)

    def handle_msg(self, peer, **kwargs):
        self.last_message_timestamp = time.time()
        logging.debug("got message, new last timestamp is %d", int(self.last_message_timestamp))

    def handle_msg_pong(self, peer, nonce, **kwargs):
        logging.info("got pong %s", nonce)
        self.ping_nonces.discard(nonce)

    def handle_msg_ping(self, peer, nonce, **kwargs):
        logging.info("got ping %s", nonce)
        peer.send_msg("pong", nonce=nonce)
