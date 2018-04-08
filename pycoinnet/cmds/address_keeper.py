#!/usr/bin/env python

"""
This bitcoin client does little more than try to keep an up-to-date
list of available clients in a text file "addresses".
"""

import asyncio
import logging
import random

from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.networks import MAINNET

from pycoinnet.cmds.common import (
    init_logging, peer_connect_pipeline
)


class AddressDB(object):
    def __init__(self, path):
        self.path = path
        self.addresses = self.load_addresses()
        self.shuffled = []

    def load_addresses(self):
        """
        Return an array of (host, port, timestamp) triples.
        """
        addresses = {}
        try:
            with open(self.path) as f:
                for l in f:
                    timestamp, host, port = l[:-1].split("/")
                    timestamp = int(timestamp)
                    port = int(port)
                    addresses[(host, port)] = timestamp
        except Exception:
            logging.error("can't open %s, using default", self.path)
            for h in [
                "bitseed.xf2.org", "dnsseed.bluematt.me",
                "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org"
            ]:
                addresses[(h, 8333)] = 1
        return addresses

    def next_address(self):
        if len(self.shuffled) == 0:
            self.shuffled = list(self.addresses.keys())
            random.shuffle(self.shuffled)
        return self.shuffled.pop()

    def remove_address(self, host, port):
        key = (host, port)
        del self.addresses[key]

    def add_address(self, host, port, timestamp):
        key = (host, port)
        old_timestamp = self.addresses.get(key) or timestamp
        self.addresses[key] = max(timestamp, old_timestamp)

    def add_addresses(self, addresses):
        for timestamp, host, port in addresses:
            self.add_address(host, port, timestamp)

    def save(self):
        if len(self.addresses) < 2:
            logging.error("too few addresses: not overwriting")
            return
        with open(self.path, "w") as f:
            for host, port in self.addresses:
                f.write(
                    "%d/%s/%d\n" % (self.addresses[(host, port)], host, port))


async def keep_minimum_connections(network, min_connection_count=4):
    address_db = AddressDB("addresses.txt")

    peer_q = peer_connect_pipeline(network)

    async def get_addresses(peer, q):
        host, port = peer.peername()

        while True:
            name, data = await peer.next_message()
            if name == 'ping':
                peer.send_msg("pong", nonce=data["nonce"])
            if name == 'addr':
                date_address_tuples = data["date_address_tuples"]
                logging.info("got %s message from %s with %d entries", name, peer, len(date_address_tuples))
                address_db.add_addresses(
                    (timestamp, address.ip_bin, address.port)
                    for timestamp, address in date_address_tuples)
                address_db.save()
                break
        peer.close()

    filters = [
        dict(callback_f=get_addresses, input_q=peer_q, worker_count=min_connection_count),
    ]
    q = MappingQueue(*filters)
    await asyncio.sleep(60)
    del q


def main():
    init_logging()
    asyncio.get_event_loop().run_until_complete(keep_minimum_connections(MAINNET))


main()
