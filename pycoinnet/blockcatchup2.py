import asyncio
import logging

from pycoinnet.aitertools import (
    iter_to_aiter, stoppable_aiter,
    push_aiter, join_aiters, gated_aiter, preload_aiter
)

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.PeerManager import PeerManager

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter
from pycoinnet.header_improvements import header_improvements_aiter
from pycoinnet.peer_pipeline import make_remote_host_aiter
from pycoinnet.pong_task import create_pong_task

from .BlockBatcher import BlockBatcher


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],
        [541920, "00000000000000000018e81687fe77a03b0cfd5287ed2a365b9664b98c9d0fbc", 541921]]'''


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


async def lifecycle_peer(limiting_remote_host_aiter, rate_limiter, desired_host_count):
    rate_limiter.push(desired_host_count*3)
    #rate_limiter.stop()
    async for _ in limiting_remote_host_aiter:
        yield _

    """
    peers = set()

    async def ensure_enough():
        pass

    async for peer in limiting_remote_host_aiter:
        peers.add()
        yield peer
    """


async def collect_blocks(network):
    blockchain_view = BlockChainView.from_json(BCV_JSON)
    blockchain_view = BlockChainView()

    host_port_q_aiter = push_aiter()
    host_port_q_aiter.stop()
    host_port_q_aiter = iter_to_aiter([("localhost", 8333)])

    dns_aiter = dns_bootstrap_host_port_aiter(network)
    #dns_aiter = iter_to_aiter([])

    remote_host_aiter = join_aiters(iter_to_aiter([dns_aiter, host_port_q_aiter]))

    limiting_remote_host_aiter = gated_aiter(remote_host_aiter)

    remote_host_aiter = make_remote_host_aiter(
        network, limiting_remote_host_aiter, version_dict=dict(version=70016))

    connected_remote_aiter = lifecycle_peer(remote_host_aiter, limiting_remote_host_aiter, 8)

    peer_manager = PeerManager(connected_remote_aiter)

    pong_task = create_pong_task(peer_manager)

    async for peer, block, index in blockcatchup(peer_manager, blockchain_view):
        print("%6d: %s (%s)" % (index, block.id(), peer))
    await peer_manager.close_all()
    await pong_task


async def blockcatchup(peer_manager, blockchain_view):
    block_batcher = BlockBatcher(peer_manager)

    hi_aiter = header_improvements_aiter(peer_manager, blockchain_view, block_batcher)

    async for idx, f in preload_aiter(1500, block_batcher.block_futures_for_header_info_aiter(hi_aiter)):
        peer, block = await f
        yield peer, block, idx


def main():
    init_logging()
    from pycoin.networks.registry import network_for_netcode
    network = network_for_netcode("btc")
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(collect_blocks(network))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())


if __name__ == "__main__":
    main()
