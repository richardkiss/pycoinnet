import asyncio
import logging
import os
import time

from pycoinnet.PeerAddress import PeerAddress

logging = logging.getLogger("standards")


class BitcoinProtocolError(Exception):
    pass


def manage_connection_count(address_queue, protocol_factory, connection_count=4):
    """
    address_queue: a queue of (host, port) tuples
    protocol_factory: the callback passed to EventLoop.create_connection
    connection_count: number of connections to keep established
    """
    event_q = asyncio.Queue()

    @asyncio.coroutine
    def run():
        while True:
            timestamp, peer_addr = yield from address_queue.get()
            host, port = peer_addr.host(), peer_addr.port
            logging.debug("got %s:%d from connection pool", host, port)
            logging.info("connecting to %s:%d" % (host, port))
            try:
                transport, protocol = yield from asyncio.get_event_loop().create_connection(
                    protocol_factory, host=host, port=port)
                logging.info("connected (tcp) to %s:%d", host, port)
                event_q.put_nowait(("connect", (host, port), protocol))
                yield from asyncio.wait_for(protocol.connection_lost_future, timeout=None)
                event_q.put_nowait(("disconnect", (host, port), protocol))
            except Exception:
                logging.exception("failed to connect to %s:%d", host, port)

    for i in range(connection_count):
        asyncio.Task(run())

    asyncio.Task(run())
    return event_q


@asyncio.coroutine
def create_server(protocol_factory, port):
    """
    Listen on a port and create new peers on that connection.
    """
    abstract_server = yield from asyncio.get_event_loop().create_server(
        protocol_factory=protocol_factory, port=port)
    return abstract_server


def version_data_for_peer(
        peer, version=70001, local_ip="127.0.0.1", local_port=6111, last_block_index=0,
        nonce=None, subversion=b"/Notoshi/", timestamp=None, want_relay=True):
    remote_ip, remote_port = peer.peername
    remote_addr = PeerAddress(1, remote_ip, remote_port)
    local_addr = PeerAddress(1, local_ip, local_port)
    nonce = nonce or int.from_bytes(os.urandom(8), byteorder="big")
    timestamp = timestamp or int(time.time())
    d = dict(
        version=70001, subversion=subversion, services=1, timestamp=timestamp,
        remote_address=remote_addr, local_address=local_addr,
        nonce=nonce,
        last_block_index=last_block_index, want_relay=want_relay
    )
    return d


@asyncio.coroutine
def initial_handshake(peer, version_parameters):
    # do handshake

    next_message = peer.new_get_next_message_f()
    peer.send_msg("version", **version_parameters)

    message_name, version_data = yield from next_message()
    if message_name != 'version':
        raise BitcoinProtocolError("missing version")
    peer.send_msg("verack")

    message_name, data = yield from next_message()
    if message_name != 'verack':
        raise BitcoinProtocolError("missing verack")

    logging.info("handshake complete with %s, version_data => %s", peer, version_data)
    return version_data


def install_ping_manager(peer, heartbeat_rate=60, missing_pong_disconnect_timeout=60):
    @asyncio.coroutine
    def ping_task(next_message):
        while True:
            try:
                yield from asyncio.wait_for(next_message(), timeout=heartbeat_rate)
                continue
            except asyncio.TimeoutError:
                pass
            # oh oh! no messages
            # send a ping
            nonce = int.from_bytes(os.urandom(8), byteorder="big")
            peer.send_msg("ping", nonce=nonce)
            end_time = time.time() + missing_pong_disconnect_timeout
            while True:
                try:
                    timeout = end_time - time.time()
                    name, data = yield from asyncio.wait_for(next_message(), timeout=timeout)
                    if name == "pong" and data["nonce"] == nonce:
                        break
                except asyncio.TimeoutError:
                    peer.connection_lost(None)
                    logging.error("remote peer %s didn't answer ping, disconnecting", peer)
                    return
    next_message = peer.new_get_next_message_f()
    asyncio.Task(ping_task(next_message))


def install_pong_manager(peer):
    @asyncio.coroutine
    def pong_task(next_message):
        while True:
            name, data = yield from next_message()
            assert name == 'ping'
            peer.send_msg("pong", nonce=data["nonce"])
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'ping')
    asyncio.Task(pong_task(next_message))


def install_pingpong_manager(peer):
    install_ping_manager(peer)
    install_pong_manager(peer)


@asyncio.coroutine
def get_date_address_tuples(peer):
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'addr')
    peer.send_msg("getaddr")
    name, data = yield from next_message()
    return data["date_address_tuples"]


@asyncio.coroutine
def get_headers_hashes(peer, after_block_hash):
    hashes = [after_block_hash]
    peer.send_msg(message_name="getheaders", version=1, hashes=hashes, hash_stop=after_block_hash)
    next_message = peer.new_get_next_message_f(lambda name, data: name == 'headers')
    name, data = yield from next_message()
    headers = [bh for bh, t in data["headers"]]
    return headers
