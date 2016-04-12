"""
Bitcoin client that connects to DNS bootstrap connections, grabs addr records,
disconnects after enough records are obtained, and that's that.
"""

import asyncio
import logging

from asyncio.queues import PriorityQueue

from pycoinnet.helpers.standards import initial_handshake
from pycoinnet.helpers.standards import get_date_address_tuples
from pycoinnet.helpers.standards import version_data_for_peer
from pycoinnet.peer.BitcoinPeerProtocol import BitcoinPeerProtocol


def dns_bootstrap_host_port_q(network_info):
    dns_bootstrap = network_info["DNS_BOOTSTRAP"]

    superpeer_ip_queue = asyncio.Queue()

    @asyncio.coroutine
    def bootstrap_superpeer_addresses(dns_bootstrap):
        for h in dns_bootstrap:
            try:
                r = yield from asyncio.get_event_loop().getaddrinfo(h, network_info["DEFAULT_PORT"])
                results = set(t[-1][:2] for t in r)
                for t in results:
                    yield from superpeer_ip_queue.put(t)
                    logging.debug("got address %s", t)
            except Exception:
                logging.exception("problem in bootstrap_superpeer_addresses")
        yield from superpeer_ip_queue.put(None)

    superpeer_ip_queue.task = asyncio.Task(bootstrap_superpeer_addresses(dns_bootstrap))
    return superpeer_ip_queue


def new_queue_of_timestamp_peeraddress_tuples(network_info, timestamp_peeraddress_tuple_queue=None):
    """
    Returns a queue which is populated with (time, host, port) tuples of
    addresses of regular peers that we can connect to.

    This works by connecting to superpeers at the DNS addresses passed and
    fetching addr records. Once we have enough, stop.
    """

    magic_header = network_info["MAGIC_HEADER"]

    superpeer_ip_queue = dns_bootstrap_host_port_q(network_info)

    if timestamp_peeraddress_tuple_queue is None:
        timestamp_peeraddress_tuple_queue = PriorityQueue()

    @asyncio.coroutine
    def loop_connect_to_superpeer(superpeer_ip_queue):
        while 1:
            try:
                pair = yield from superpeer_ip_queue.get()
                if pair is None:
                    break
                peer_name = "%s:%d" % pair
                host, port = pair
                logging.debug("connecting to superpeer at %s", peer_name)
                transport, peer = yield from asyncio.get_event_loop().create_connection(
                    lambda: BitcoinPeerProtocol(magic_header), host=host, port=port)

                logging.debug("connected to superpeer at %s", peer_name)
                yield from initial_handshake(peer, version_data_for_peer(peer))
                logging.debug("handshake complete on %s", peer_name)

                date_address_tuples = yield from get_date_address_tuples(peer)

                logging.debug("got addresses from %s", peer_name)
                for da in date_address_tuples:
                    timestamp_peeraddress_tuple_queue.put_nowait((-da[0], da[1]))
                logging.debug("closing connection to %s", peer_name)
                transport.close()
            except Exception:
                logging.exception("failed during connect to %s", peer_name)

    timestamp_peeraddress_tuple_queue.tasks = [
        asyncio.Task(loop_connect_to_superpeer(superpeer_ip_queue)) for i in range(30)]

    return timestamp_peeraddress_tuple_queue


@asyncio.coroutine
def show(timestamp_address_queue):
    while 1:
        timestamp, addr = yield from timestamp_address_queue.get()
        logging.info("@ %s with address %s", timestamp, addr)


def main():
    asyncio.tasks._DEBUG = True
    logging.basicConfig(
        level=logging.DEBUG,
        format=('%(asctime)s [%(process)d] [%(levelname)s] '
                '%(filename)s:%(lineno)d %(message)s'))
    timestamp_address_queue = new_queue_of_timestamp_peeraddress_tuples()
    asyncio.Task(show(timestamp_address_queue))
    asyncio.get_event_loop().run_forever()

if __name__ == '__main__':
    main()
