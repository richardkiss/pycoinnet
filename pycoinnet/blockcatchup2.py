import asyncio
import logging

from pycoinnet.aitertools import (
    aiter_forker, iter_to_aiter, stoppable_aiter,
    push_aiter, map_aiter, join_aiters, gated_aiter, preload_aiter
)

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_aiter
from pycoinnet.peer_pipeline import make_remote_host_aiter

from .BlockBatcher import BlockBatcher


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],
        [541920, "00000000000000000018e81687fe77a03b0cfd5287ed2a365b9664b98c9d0fbc", 541921]]'''


def create_pong_manager(peer_manager):

    async def task():
        async for peer, name, data in peer_manager.new_event_aiter():
            if name == "ping":
                peer.send_msg("pong", nonce=data["nonce"])

    return asyncio.ensure_future(task())


def event_aiter_from_peer_aiter(peer_aiter):
    async def peer_to_events(peer):
        async def add_peer(event):
            name, data = event
            return peer, name, data
        return map_aiter(add_peer, peer.event_aiter())
    return join_aiters(map_aiter(peer_to_events, peer_aiter))


class PeerManager:
    def __init__(self, peer_aiter):
        self._active_peers = set()
        self._peer_aiter_forker = aiter_forker(peer_aiter)
        self._event_aiter_forker = aiter_forker(event_aiter_from_peer_aiter(self.new_peer_aiter()))
        self._watcher_task = asyncio.ensure_future(self._watcher())

    async def _watcher(self):
        peer_aiter = self.new_peer_aiter(is_active=False)
        async for peer in peer_aiter:
            self._active_peers.add(peer)
        for peer in list(self._active_peers):
            await peer.wait_until_close()

    async def close_all(self):
        async for peer in self.new_peer_aiter():
            peer.close()
        await self._watcher_task

    def new_peer_aiter(self, is_active=True):
        return join_aiters(iter_to_aiter([
            self.active_peers_aiter(),
            self._peer_aiter_forker.fork(is_active=is_active)]))

    def new_event_aiter(self, is_active=True):
        return self._event_aiter_forker.fork(is_active=is_active)

    def active_peers_aiter(self):
        return iter_to_aiter([_ for _ in self._active_peers if not _.is_closing()])


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

    pong_task = create_pong_manager(peer_manager)

    async for peer, block, index in blockcatchup(peer_manager, blockchain_view):
        print("%6d: %s (%s)" % (index, block.id(), peer))
    await peer_manager.close_all()
    await pong_task


async def monitor_improvements(peer_manager, blockchain_view, block_batcher):
    async for peer, message, data in peer_manager.new_event_aiter():
        if message != "headers":
            continue

        headers = [bh for bh, t in data["headers"]]

        while (len(headers) > 0 and
                headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1] and
                blockchain_view.tuple_for_hash(headers[0].hash()) is None):
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await block_batcher._add_to_download_queue(bh, 0)
            peer, block = await f
            headers = [block] + headers

        if len(headers) > 0:
            block_number = blockchain_view.do_headers_improve_path(headers)
            if block_number is False:
                continue

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(blockchain_view.last_block_index()+1-block_number):
            the_tuple = blockchain_view.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])

        logging.info("got %d new header(s) starting at %d" % (len(hashes), block_number))
        yield peer, block_number, hashes


async def headers_info_aiter(peer_manager, blockchain_view, block_batcher):
    """
    yields pairs of (block_header_list, block_index)

    stops when peer manager runs out of peers
    """
    caught_up_peers = set()

    aiter_of_aiters = push_aiter()
    joined_aiter = join_aiters(aiter_of_aiters)

    def request_headers_from_peer(peer, blockchain_view):
        if peer in caught_up_peers:
            return
        block_locator_hashes = blockchain_view.block_locator_hashes()
        hash_stop = blockchain_view.hash_initial_block()
        logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])
        peer.send_msg("getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)

    async def peer_aiter_to_triple(aiter):
        async for _ in aiter:
            yield _, None, None

    async def make_alt_peer_aiter(peer_manager, peer):
        while True:
            async for _ in peer_manager.new_peer_aiter():
                if _ != peer:
                    break

            async for _ in peer_manager.active_peers_aiter():
                if _ != peer:
                    yield _

    monitor_improvements_aiter = aiter_forker(
        monitor_improvements(peer_manager, blockchain_view, block_batcher))

    alt_peer_aiter = stoppable_aiter(peer_manager.new_peer_aiter())
    aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))

    aiter_of_aiters.push(monitor_improvements_aiter.fork())

    best_peer = None

    async for peer, block_index, hashes in joined_aiter:
        if block_index is None:
            request_headers_from_peer(peer, blockchain_view)
            continue

        yield (peer, block_index, hashes)

        # case 1: we got a message from something other than our best_peer
        if best_peer != peer:
            logging.debug("got a new best headers peer %s (was %s)", peer, best_peer)
            if best_peer is None:
                alt_peer_aiter.stop()
            else:
                # cancel old best peer
                alt_peer_aiter.stop()

            if len(hashes) > 0:
                # case 1a: we got an improvement so we now have a best_peer
                best_peer = peer

                # retry this peer
                aiter_of_aiters.push(peer_aiter_to_triple(iter_to_aiter([best_peer])))

                # set up the alt peer aiter
                alt_peer_aiter = gated_aiter(make_alt_peer_aiter(peer_manager, peer))
                aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))
                alt_peer_aiter.push(1)
            # case 1b: we didn't get an improvement. Just keep waiting
            continue

        # case 2: we got a message from our best_peer
        else:
            if len(hashes) > 0:
                # 2a: we got an improvement
                # query it again
                aiter_of_aiters.push(peer_aiter_to_triple(iter_to_aiter([best_peer])))
                # and get another alt peer
                alt_peer_aiter.push(1)
                continue
            # 2b: we ran out. Try all peers
            alt_peer_aiter.stop()
            best_peer = None
            alt_peer_aiter = stoppable_aiter(peer_manager.new_peer_aiter())
            aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))
            continue


async def blockcatchup(peer_manager, blockchain_view):
    block_batcher = BlockBatcher(peer_manager)

    hi_aiter = headers_info_aiter(peer_manager, blockchain_view, block_batcher)

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
