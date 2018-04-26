import argparse
import asyncio
import logging

from pycoin.message.InvItem import ITEM_TYPE_TX
from pycoin.networks.registry import network_for_netcode

from pycoinnet.cmds.common import init_logging, peer_connect_pipeline
from pycoinnet.pong_manager import install_pong_manager


async def delegater(network, pp1, pp2):
    peer_pipeline = peer_connect_pipeline(network, version_dict=dict(version=70016))
    while 1:
        peer = await peer_pipeline.get()
        await pp1.put(peer)
        pp1, pp2 = pp2, pp1
    peer_pipeline.stop()


async def sender(peer_pipeline, tx):
    while 1:
        peer = await peer_pipeline.get()
        peer.send_msg("tx", tx=tx)
        peer.close()


async def receiver(peer_pipeline, tx, tx_future):

    tx_hash = tx.hash()

    def handle_inv(peer, name, data):
        for item in data["items"]:
            if item.item_type == ITEM_TYPE_TX:
                logging.debug("saw tx id %s", item.data)
                if item.data == tx_hash and not tx_future.done():
                    logging.info("saw tx id %s at %s", item.data, peer)
                    tx_future.set_result(item)

    def ignore(peer, name, data):
        pass

    while not tx_future.done():
        peer = await peer_pipeline.get()
        install_pong_manager(peer)
        peer.set_request_callback("inv", handle_inv)
        for msg in "feefilter sendcmpct addr sendheaders".split():
            peer.set_request_callback(msg, ignore)
        peer.send_msg("mempool")
        peer.start()


async def pushtx(args):
    network = args.network
    tx = network.tx.from_hex(args.tx_hex)

    pp1 = asyncio.Queue()
    pp2 = asyncio.Queue()

    delegate_task = asyncio.ensure_future(delegater(network, pp1, pp2))

    send_task = asyncio.ensure_future(sender(pp1, tx))

    tx_future = asyncio.Future()

    receive_task = asyncio.ensure_future(receiver(pp2, tx, tx_future))

    r = await tx_future
    send_task.cancel()
    delegate_task.cancel()
    receive_task.cancel()


def main():
    init_logging(logging.INFO)
    parser = argparse.ArgumentParser(description="Transmit a transaction to the network")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('tx_hex', help='transaction hex')

    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(pushtx(args))


if __name__ == '__main__':
    main()
