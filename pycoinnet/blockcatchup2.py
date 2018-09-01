import asyncio
import logging

from pycoinnet.aitertools import (
    azip, aiter_forker, flatten_aiter, iter_to_aiter,
    q_aiter, map_aiter, parallel_map_aiter, join_aiters, rated_aiter
)

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_iterator as dns_bootstrap_host_port_aiter
from pycoinnet.peer_pipeline import connected_peer_iterator as make_remote_host_aiter

LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)



def peek(callback_f, aiter):
    return aiter
    forked = aiter_forker(aiter)
    fork = forked.new_fork()

    async def task(fork):
        async for _ in fork:
            breakpoint()
            callback_f(_)

    # LAME
    forked.task = asyncio.ensure_future(task(fork))
    return forked


async def lifecycle_peer(limiting_remote_host_aiter, rate_limiter, desired_host_count):
    rate_limiter.push_nowait(desired_host_count*3)
    rate_limiter.stop()
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


BCV_JSON = '[[0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],[539466, "000000000000000000007f3c3daa3ed0aded5069cb3ef1c1922b0195ef437138", 539467]]'


async def collect_blocks(network):

    host_port_q_aiter = q_aiter()
    host_port_q_aiter.stop()
    #host_port_q_aiter = iter_to_aiter([("192.168.1.99", 8333)])
    dns_aiter = dns_bootstrap_host_port_aiter(network)
    #dns_aiter = iter_to_aiter([])
    remote_host_aiter = join_aiters(iter_to_aiter([dns_aiter, host_port_q_aiter]))
    blockchain_view = BlockChainView.from_json(BCV_JSON)

    rate_limiter = q_aiter()
    limiting_remote_host_aiter = rated_aiter(rate_limiter, remote_host_aiter)

    remote_host_aiter = make_remote_host_aiter(network, limiting_remote_host_aiter, version_dict=dict(version=70016))

    connected_remote_aiter = lifecycle_peer(remote_host_aiter, rate_limiter, 8)

    async for _ in blockcatchup(connected_remote_aiter, blockchain_view, peer_count=3):
        print(_)


async def headers_info_aiter(peer_aiter, event_aiter, blockchain_view, peer_count):

    caught_up_peers = set()

    async for peer in peer_aiter:
        while True:
            headers = []
            block_locator_hashes = blockchain_view.block_locator_hashes()
            hash_stop = blockchain_view.hash_initial_block()
            logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])

            peer.send_msg("getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)

            async for p, message, data in event_aiter:
                if message == "headers":
                    break
            headers = [bh for bh, t in data["headers"]]

            while (len(headers) > 0 and
                    headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1]):
                # this hack is necessary because the stupid default client
                # does not send the genesis block!
                bh = headers[0].previous_block_hash
                f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
                block = await f
                headers = [block] + headers

            block_number = blockchain_view.do_headers_improve_path(headers)
            if block_number is False:
                # this peer has exhausted its view
                caught_up_peers.add(peer)
                break

            logging.debug("block header count is now %d", block_number)
            hashes = []

            for idx in range(blockchain_view.last_block_index()+1-block_number):
                the_tuple = blockchain_view.tuple_for_index(idx+block_number)
                assert the_tuple[0] == idx + block_number
                assert headers[idx].hash() == the_tuple[1]
                hashes.append(headers[idx])

            logging.info("got %d new header(s) starting at %d" % (len(hashes), block_number))
            for _, bh in enumerate(hashes):
                yield (bh, block_number + _)
        if len(caught_up_peers) >= peer_count:
            break


async def headers_info_aiter_to_block(peer_aiter, event_aiter, headers_info_aiter, filter_f=None):

    async for peer in peer_aiter:
        break

    async for block_header, block_index in headers_info_aiter:
        item_type = filter_f(block_header, block_index)
        if not item_type:
            yield (block_header, block_index)
        else:
            inv_item = InvItem(item_type, block_header.hash())
            peer.send_msg("getdata", items=[inv_item])
            async for p, message, data in event_aiter:
                if p != peer:
                    continue
                if message == "block":
                    block = data["block"]
                    if block.hash() == block_header.hash():
                        yield (block, block_index)
                        break



async def blockcatchup(peer_aiter, blockchain_view, peer_count, filter_f=None):
    filter_f = filter_f or (lambda block_hash, index: ITEM_TYPE_BLOCK)

    def _monitor_peer_aiter(peer_aiter):
        async def peer_to_events(peer):
            async def add_peer(event):
                name, data = event
                return peer, name, data
            return map_aiter(add_peer, peer.event_aiter())
        return map_aiter(peer_to_events, peer_aiter)

    peer_aiter = aiter_forker(peer_aiter)

    event_aiter = aiter_forker(join_aiters(_monitor_peer_aiter(peer_aiter)))

    async def go():
        async for _ in event_aiter:
            pass

    task = asyncio.ensure_future(go())

    hi_aiter = headers_info_aiter(peer_aiter.new_fork(), event_aiter.new_fork(), blockchain_view, 3)

    block_aiter = headers_info_aiter_to_block(peer_aiter.new_fork(), event_aiter.new_fork(), hi_aiter, filter_f)

    async for _ in block_aiter:
        yield _
    


class block_batcher:
    def __init__(self, peer_aiter, peer_event_aiter):
        self._peer_aiter = peer_aiter
        self._peer_event_aiter = peer_event_aiter



    def future_for_block_hash(self, block_hash, type=ITEM_TYPE_BLOCK):
        pass


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