import argparse
import asyncio
import logging

from aiter import push_aiter, join_aiters, sharable_aiter
from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX
from pycoin.networks.registry import network_for_netcode

from pycoinnet.cmds.common import (
    init_logging, peer_manager_for_host_port_aiter, host_port_aiter_for_addresses
)
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter


async def sender(peer_manager, tx):

    inv_item = InvItem(ITEM_TYPE_TX, tx.hash())

    async def new_peer(peer_manager):
        async for peer in peer_manager.new_peer_aiter():
            asyncio.get_event_loop().call_later(10, peer.close)
            peer.send_msg("inv", items=[inv_item])

    task = asyncio.ensure_future(new_peer(peer_manager))

    async for peer, name, data in peer_manager.new_event_aiter():
        if name == "getdata":
            if inv_item in data["items"]:
                logging.info("sending tx to %s", peer)
                peer.send_msg("tx", tx=tx)
        if name == "reject":
            logging.info(
                "reject: %s 0x%x %s %s", data["message"], data["code"], data["reason"], data["data"])

    await task


async def watcher(peer_manager, tx):

    tx_hash = tx.hash()

    async def new_peer(peer_manager):
        async for peer in peer_manager.new_peer_aiter():
            asyncio.get_event_loop().call_later(120, peer.close)
            peer.send_msg("feefilter", fee_filter_value=100)
            peer.send_msg("mempool")

    task = asyncio.ensure_future(new_peer(peer_manager))

    async for peer, name, data in peer_manager.new_event_aiter():
        if name == "inv":
            for item in data["items"]:
                if item.item_type == ITEM_TYPE_TX:
                    logging.debug("saw tx id %s", item.data)
                    if item.data == tx_hash:
                        logging.info("saw tx id %s at %s", item.data, peer)
                        task.cancel()
                        return item


def peer_managers_for_args(args):
    network = args.network

    host_port_aiter_of_aiters = push_aiter()
    host_port_aiter_of_aiters.push(dns_bootstrap_host_port_aiter(network))
    host_port_aiter_of_aiters.stop()
    dns_host_port_aiter = sharable_aiter(join_aiters(host_port_aiter_of_aiters))

    sender_peer_manager = peer_manager_for_host_port_aiter(network, dns_host_port_aiter)
    if args.peer:
        host_port_aiter = host_port_aiter_for_addresses(args.peer, network.default_port)
        watcher_peer_manager = peer_manager_for_host_port_aiter(network, host_port_aiter)
    else:
        watcher_peer_manager = peer_manager_for_host_port_aiter(network, dns_host_port_aiter)
    return sender_peer_manager, watcher_peer_manager


async def pushtx(args):
    network = args.network
    tx_info = args.tx
    try:
        tx = network.tx.parse(open(tx_info, "rb"))
    except Exception:
        tx = network.tx.from_hex(tx_info)

    # there is a sender PeerManager and a watcher PeerManager

    sender_peer_manager, watcher_peer_manager = peer_managers_for_args(args)

    send_task = asyncio.ensure_future(sender(sender_peer_manager, tx))

    await watcher(watcher_peer_manager, tx)
    await asyncio.wait([
        sender_peer_manager.close_all(),
        watcher_peer_manager.close_all()
    ])
    await send_task


def main():
    init_logging(logging.DEBUG)
    parser = argparse.ArgumentParser(description="Transmit a transaction to the network")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('-p', "--peer", help='initial peer', nargs="?")
    parser.add_argument('tx', help='transaction hex or tx file')

    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(pushtx(args))


if __name__ == '__main__':
    main()
