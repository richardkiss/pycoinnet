import argparse
import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX
from pycoin.networks.registry import network_for_netcode

from pycoinnet.cmds.common import init_logging, peer_connect_pipeline
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.pong_manager import install_pong_manager


async def delegater(peer_pipeline, pp1, pp2):
    while 1:
        peer = await peer_pipeline.get()
        await pp1.put(peer)
        pp1, pp2 = pp2, pp1
    peer_pipeline.stop()


def install_ignore_manager(peer, also_ignore=[]):

    def ignore(peer, name, data):
        pass

    for msg in "feefilter sendcmpct addr sendheaders".split() + also_ignore:
        peer.set_request_callback(msg, ignore)


async def sender(peer_pipeline, tx, handle_addr):

    inv_item = InvItem(ITEM_TYPE_TX, tx.hash())

    def handle_get_data(peer, name, data):
        if inv_item in data["items"]:
            logging.info("sending tx to %s", peer)
            peer.send_msg("tx", tx=tx)

    def handle_reject(peer, name, data):
        logging.info("reject: %s 0x%x %s %s", data["message"], data["code"], data["reason"], data["data"])

    while 1:
        peer = await peer_pipeline.get()
        install_pong_manager(peer)
        install_ignore_manager(peer, ["inv"])
        peer.set_request_callback("reject", handle_reject)
        peer.set_request_callback("getdata", handle_get_data)
        peer.set_request_callback("addr", handle_addr)
        peer.send_msg("getaddr")
        peer.start()
        peer.send_msg("inv", items=[inv_item])
        asyncio.get_event_loop().call_later(10, peer.close)


async def receiver(peer_pipeline, tx, tx_future, handle_addr):

    tx_hash = tx.hash()

    def handle_inv(peer, name, data):
        for item in data["items"]:
            if item.item_type == ITEM_TYPE_TX:
                logging.debug("saw tx id %s", item.data)
                if item.data == tx_hash and not tx_future.done():
                    logging.info("saw tx id %s at %s", item.data, peer)
                    tx_future.set_result(item)

    while not tx_future.done():
        peer = await peer_pipeline.get()
        install_pong_manager(peer)
        install_ignore_manager(peer)
        peer.set_request_callback("inv", handle_inv)
        peer.set_request_callback("addr", handle_addr)
        peer.send_msg("feefilter", fee_filter_value=100)
        peer.send_msg("mempool")
        peer.send_msg("getaddr")
        peer.start()
        asyncio.get_event_loop().call_later(120, peer.close)


async def pushtx(args):
    network = args.network
    tx_info = args.tx
    try:
        tx = network.tx.parse(open(tx_info, "rb"))
    except Exception:
        tx = network.tx.from_hex(tx_info)

    pp1 = asyncio.Queue()
    pp2 = asyncio.Queue()

    if args.peer:
        host_q = asyncio.Queue()
        await host_q.put((args.peer, args.network.default_port))
    else:
        host_q = dns_bootstrap_host_port_q(network)
    peer_pipeline = peer_connect_pipeline(args.network, host_q=host_q, version_dict=dict(version=70015))

    delegate_task = asyncio.ensure_future(delegater(peer_pipeline, pp1, pp2))

    def handle_addr(peer, name, data):
        for pair in data["date_address_tuples"]:
            tuple = (pair[1].host(), pair[1].port)
            host_q.put_nowait(tuple)

    send_task = asyncio.ensure_future(sender(pp1, tx, handle_addr))

    tx_future = asyncio.Future()

    receive_task = asyncio.ensure_future(receiver(pp2, tx, tx_future, handle_addr))

    r = await tx_future
    send_task.cancel()
    delegate_task.cancel()
    receive_task.cancel()


def main():
    init_logging(logging.INFO)
    parser = argparse.ArgumentParser(description="Transmit a transaction to the network")
    parser.add_argument('-n', "--network", help='specify network', type=network_for_netcode,
                        default=network_for_netcode("BTC"))
    parser.add_argument('-p', "--peer", help='initial peer', nargs="?")
    parser.add_argument('tx', help='transaction hex or tx file')

    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(pushtx(args))


if __name__ == '__main__':
    main()
