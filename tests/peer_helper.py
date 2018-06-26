import asyncio

from pycoinnet.Peer import Peer
from pycoinnet.version import version_data_for_peer

from tests.pipes import create_pipe_streams_pair


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


def create_peer_pair(network):
    (r1, w1), (r2, w2) = run(create_pipe_streams_pair())
    p1 = Peer(r1, w1, network.magic_header, network.parse_message, network.pack_message)
    p2 = Peer(r2, w2, network.magic_header, network.parse_message, network.pack_message)
    return p1, p2


def create_handshaked_peer_pair(network):
    p1, p2 = create_peer_pair(network)
    t1 = p1.perform_handshake(**version_data_for_peer(remote_ip="10.0.0.1", remote_port=8333))
    t2 = p2.perform_handshake(**version_data_for_peer(remote_ip="10.0.0.2", remote_port=8333))
    run(asyncio.wait([t1, t2]))
    p1.start()
    p2.start()
    return p1, p2
