import asyncio
import logging

from pycoinnet.aitertools import (
    sharable_aiter, iter_to_aiter, push_aiter, join_aiters, gated_aiter, preload_aiter, map_aiter, azip
)

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.PeerManager import PeerManager

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter
from pycoinnet.header_improvements import header_improvements_aiter
from pycoinnet.peer_pipeline import make_handshaked_peer_aiter
from pycoinnet.pong_task import create_pong_task

from .BlockBatcher import BlockBatcher


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],
        [542157, "00000000000000000009f163eade1888fc7c51e52a89967129df60bc1a53bc15", 542158]]'''


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


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


async def collect_blocks(network):
    blockchain_view = BlockChainView.from_json(BCV_JSON)
    blockchain_view = BlockChainView()

    host_port_q_aiter = push_aiter()
    host_port_q_aiter.stop()
    host_port_q_aiter = iter_to_aiter([("localhost", 8333)])

    dns_aiter = dns_bootstrap_host_port_aiter(network)
    #dns_aiter = iter_to_aiter([])

    dns_aiter = map_aiter(lambda _: _[1], azip(iter_to_aiter(range(10)), dns_aiter))
    limiting_remote_host_aiter = gated_aiter(join_aiters(iter_to_aiter([dns_aiter, host_port_q_aiter])))

    peer_manager = peer_manager_for_host_port_aiter(network, limiting_remote_host_aiter)

    pong_task = create_pong_task(peer_manager)

    async for peer, block, index in blockcatchup(peer_manager, blockchain_view, limiting_remote_host_aiter):
        print("%6d: %s (%s)" % (index, block.id(), peer))
    await peer_manager.close_all()
    await pong_task


async def blockcatchup(peer_manager, blockchain_view, limiting_remote_host_aiter):

    block_batcher = BlockBatcher(peer_manager)

    async def header_improvements(peer_manager, blockchain_view, block_batcher, count=4):
        hia = header_improvements_aiter(peer_manager, blockchain_view, block_batcher, count)
        caught_up_peers = set()
        async for peer, block_index, hashes in hia:
            if len(hashes) == 0:
                #caught_up_peers.add(peer)
                if len(caught_up_peers) >= count:
                    break
            yield peer, block_index, hashes
        limiting_remote_host_aiter.stop()

    async for idx, f in preload_aiter(2500, block_batcher.block_futures_for_header_info_aiter(
            header_improvements(peer_manager, blockchain_view, block_batcher))):
        peer, block = await f
        yield peer, block, idx
    await peer_manager.close_all()
    await block_batcher.join()


def main():
    init_logging()
    from pycoin.networks.registry import network_for_netcode
    network = network_for_netcode("btc")
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(collect_blocks(network))
    finally:
        logging.debug("waiting for shutdown_asyncgens")
        loop.run_until_complete(loop.shutdown_asyncgens())
        logging.debug("done waiting for shutdown_asyncgens")


if __name__ == "__main__":
    main()
