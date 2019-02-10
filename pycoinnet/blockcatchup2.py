import asyncio
import logging

from aiter import (
    iter_to_aiter, push_aiter, join_aiters, gated_aiter, map_aiter, azip
)

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter
from pycoinnet.pong_task import create_pong_task

from pycoinnet.cmds.common import init_logging

from pycoinnet.blockcatchup import peer_manager_for_host_port_aiter, blockcatchup

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],
        [542157, "00000000000000000009f163eade1888fc7c51e52a89967129df60bc1a53bc15", 542158]]'''


async def collect_blocks(network):
    blockchain_view = BlockChainView.from_json(BCV_JSON)
    # blockchain_view = BlockChainView()

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
