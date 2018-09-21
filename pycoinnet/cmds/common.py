import asyncio
import logging
import os.path

from pycoinnet.aitertools import push_aiter, join_aiters, map_aiter, sharable_aiter, iter_to_aiter, gated_aiter
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter
from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.PeerManager import PeerManager
from pycoinnet.peer_pipeline import make_handshaked_peer_aiter
from pycoinnet.pong_task import create_pong_task


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


def set_log_file(logPath, level=logging.NOTSET):
    if logPath is None:
        return
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)


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


def peer_address_to_hostport(peer_address, default_port):
    if "/" in peer_address:
        host, port = peer_address.split("/", 1)
        port = int(port)
        return host, port
    return peer_address, default_port


def peer_addresses_to_host_aiter(network, peer_addresses=[]):
    if peer_addresses:
        hostports = [peer_address_to_hostport(_, network.default_port) for _ in peer_addresses]
        return iter_to_aiter(hostports)
    return dns_bootstrap_host_port_aiter(network)


async def peer_lifecycle(remote_peer_aiter, rate_limiter):
    rate_limiter.push(1)
    async for peer in remote_peer_aiter:
        logging.info("connected to %s", peer)
        yield peer
        await peer.wait_until_close()
        logging.info("close connection to %s", peer)
        rate_limiter.push(1)


def peer_manager_for_host_port_aiter(network, remote_host_aiter, count=8):

    handshaked_peer_aiter = sharable_aiter(make_handshaked_peer_aiter(
        network, remote_host_aiter, version_dict=dict(version=70016)))

    connected_remote_aiter = join_aiters(iter_to_aiter([
        peer_lifecycle(handshaked_peer_aiter, remote_host_aiter) for _ in range(count)]))

    return PeerManager(connected_remote_aiter)


def peer_manager_for_args(args, bloom_filter=None):

    network = args.network
    count = getattr(args, "count", 4)

    host_port_aiter_of_aiters = push_aiter()
    host_port_aiter_of_aiters.push(peer_addresses_to_host_aiter(network, args.peer))
    host_port_aiter_of_aiters.stop()
    host_port_aiter = join_aiters(host_port_aiter_of_aiters)

    if bloom_filter and 0:
        # BRAIN DAMAGE: bloom filter doesn't work
        filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
        flags = 1  # BLOOM_UPDATE_ALL = 1  # BRAIN DAMAGE

        async def got_new_peer(peer):
            if args.spv:
                peer.send_msg("filterload", filter=filter_bytes, tweak=tweak,
                              hash_function_count=hash_function_count, flags=flags)
            return peer

        peer_iterator = map_aiter(got_new_peer, peer_iterator)

    gated_host_aiter = gated_aiter(host_port_aiter)
    peer_manager = peer_manager_for_host_port_aiter(network, gated_host_aiter, count)
    peer_manager.pong_task = create_pong_task(peer_manager)
    return peer_manager
