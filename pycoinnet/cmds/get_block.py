import argparse
import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoin.serialize import h2b_rev

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.networks import MAINNET
from pycoinnet.cmds.common import init_logging

from pycoinnet.version import version_data_for_peer

from pycoinnet.Peer import Peer


async def find_block(host, port, block_hash, block_future, network):
    logging.info("connecting to %s:%d", host, port)
    reader, writer = await asyncio.open_connection(host=host, port=port)

    peer = Peer(reader, writer, network.magic_header, network.parse_from_data, network.pack_from_data)
    version_data = version_data_for_peer(peer)
    await peer.perform_handshake(**version_data)
    peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, block_hash)])
    while True:
        name, data = await peer.next_message(unpack_to_dict=True)
        if name == 'block':
            block_future.set_result(data["block"])


async def create_block_tasks(host_port_q, block_future, block_hash, network):
    loop = asyncio.get_event_loop()
    for _ in range(16):
        peer_addr = await host_port_q.get()
        if peer_addr is None:
            return
        logging.info("trying %s", peer_addr)
        host, port = peer_addr
        loop.create_task(find_block(host, port, block_hash, block_future, network))


async def get_block(block_id, network):
    loop = asyncio.get_event_loop()
    block_future = loop.create_future()
    loop.create_task(create_block_tasks(
        dns_bootstrap_host_port_q(network), block_future, block_id, network))
    return (await block_future)


def main():
    init_logging()
    parser = argparse.ArgumentParser(description="Fetch a block by ID.")
    parser.add_argument('id', help='Block ID as hex')

    args = parser.parse_args()

    network = MAINNET

    block = asyncio.get_event_loop().run_until_complete(get_block(h2b_rev(args.id), network))

    with open("%s.bin" % block.id(), "wb") as f:
        block.stream(f)
    print(block.id())


if __name__ == '__main__':
    main()
