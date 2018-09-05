import asyncio
import logging
import weakref

from pycoinnet.aitertools import (
    aiter_forker, iter_to_aiter, map_filter_aiter, flatten_aiter, q_aiter,
    push_aiter, map_aiter, parallel_map_aiter, join_aiters, rated_aiter
)

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK

from pycoinnet.BlockChainView import BlockChainView

from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_iterator as dns_bootstrap_host_port_aiter
from pycoinnet.peer_pipeline import connected_peer_iterator as make_remote_host_aiter

LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1],
        [539965, "0000000000000000000d0a4281ca43fc936fbd7c957a06a7354e179333421c32", 539966]]'''

BCV_JSON = '''[
        [0, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", 1]]'''


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
        return join_aiters(iter_to_aiter([iter_to_aiter(
            list([_ for _ in self._active_peers if not _.is_closing()])),
            self._peer_aiter_forker.new_fork(is_active=is_active)]))

    def new_event_aiter(self, is_active=True):
        return self._event_aiter_forker.new_fork(is_active=is_active)


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


async def lifecycle_peer(limiting_remote_host_aiter, rate_limiter, desired_host_count):
    rate_limiter.push_nowait(desired_host_count*3)
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

    host_port_q_aiter = push_aiter()
    host_port_q_aiter.stop()
    host_port_q_aiter = iter_to_aiter([("192.168.1.99", 8333)])

    dns_aiter = dns_bootstrap_host_port_aiter(network)
    #dns_aiter = iter_to_aiter([])

    remote_host_aiter = join_aiters(iter_to_aiter([dns_aiter, host_port_q_aiter]))

    rate_limiter = push_aiter()
    limiting_remote_host_aiter = rated_aiter(rate_limiter, remote_host_aiter)

    remote_host_aiter = make_remote_host_aiter(
        network, limiting_remote_host_aiter, version_dict=dict(version=70016))

    connected_remote_aiter = lifecycle_peer(remote_host_aiter, rate_limiter, 8)

    peer_manager = PeerManager(connected_remote_aiter)

    async for peer, block, index in blockcatchup(peer_manager, blockchain_view, peer_count=3):
        print("%6d: %s" % (index, block))
    await peer_manager.close_all()


async def headers_info_aiter(peer_manager, blockchain_view, peer_count):
    """
    yields pairs of (block_header, block_index)

    stops when the number of peers who claim to have caught you up is "peer_count"
    """
    caught_up_peers = set()

    event_aiter = peer_manager.new_event_aiter()

    async for peer in peer_manager.new_peer_aiter():
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
            yield (hashes, block_number)
        if len(caught_up_peers) >= peer_count:
            break


async def blockcatchup(peer_manager, blockchain_view, peer_count, filter_f=None):
    filter_f = filter_f or (lambda block_hash, index: ITEM_TYPE_BLOCK)

    hi_aiter = headers_info_aiter(peer_manager, blockchain_view, 3)

    block_batcher = BlockBatcher(peer_manager, hi_aiter)

    async for idx, f in block_batcher:
        peer, block = await f
        yield peer, block, idx


class BlockBatcher:
    def __init__(self, peer_manager, header_info_aiter):
        self._inv_item_future_queue = asyncio.PriorityQueue()
        self._inv_item_future_aiter = q_aiter(self._inv_item_future_queue)
        self._block_hash_to_future = weakref.WeakValueDictionary()
        self._header_info_aiter = header_info_aiter
        self._peer_aiter = peer_manager.new_peer_aiter()
        self._event_aiter = peer_manager.new_event_aiter()
        self._peer_task = asyncio.ensure_future(self._peer_worker())
        self._event_task = asyncio.ensure_future(self._event_worker())

    async def _header_info_aiter_to_block_batcher_aiter(self, header_info):
        block_headers, first_block_index = header_info
        results = []
        for _, block_header in enumerate(block_headers):
            block_index = first_block_index + _
            results.append((block_index, await self._add_to_download_queue(block_header, block_index)))
        return results

    async def __aiter__(self):
        async for _ in map_filter_aiter(self._header_info_aiter_to_block_batcher_aiter, self._header_info_aiter):
            yield _
        self._inv_item_future_aiter.stop()
        self._event_task.cancel()
        await self._peer_task
        #await self._event_task

    async def _add_to_download_queue(self, block_header, block_index):
        block_hash = block_header.hash()
        f = self._block_hash_to_future.get(block_hash)
        if not f:
            f = asyncio.Future()
            self._block_hash_to_future[block_hash] = f
            item = (block_index, block_hash, f, set())
            await self._inv_item_future_aiter.push(item)
        return f

    async def _peer_worker(self):
        subtasks = set()
        async for peer in self._peer_aiter:
            # make two "fetch" tasks
            subtasks.update([asyncio.ensure_future(self._peer_batch_task(peer)) for _ in range(2)])
        done, pending = await asyncio.wait(subtasks)

    async def _get_batch(self, peer, desired_batch_size):
        batch = []
        skipped = []

        async for item in self._inv_item_future_aiter:
            (priority, block_hash, f, peers_tried) = item
            if f.done():
                continue
            if peer in peers_tried:
                skipped.append(item)
                continue
            peers_tried.add(peer)
            batch.append(item)
            if self._inv_item_future_queue.empty() or len(batch) >= desired_batch_size:
                break

        if len(batch) > 0:
            logging.debug("peer %s built batch starting with %d with size %d (max %d)",
                          peer, batch[0][0], len(batch), desired_batch_size)

        for item in skipped:
            await self._inv_item_future_aiter.push(item)

        return batch

    async def _peer_batch_task(self, peer):
        loop = asyncio.get_event_loop()
        target_batch_time = 5.0
        desired_batch_size = 1
        max_batch_size = 100
        while True:
            batch = await self._get_batch(peer, desired_batch_size)
            if len(batch) == 0:
                break
            inv_items = []
            futures = []
            for (priority, block_hash, f, peers_tried) in batch:
                inv_items.append(InvItem(ITEM_TYPE_BLOCK, block_hash))
                futures.append(f)
            start_time = loop.time()
            peer.send_msg("getdata", items=inv_items)
            loop.call_later(10, self._timeout_batch, batch)
            done, pending = await asyncio.wait(futures)
            total_time = loop.time() - start_time
            if not total_time:
                total_time = 1.0
            item_per_unit_time = len(batch) / total_time
            desired_batch_size = min(
                int(desired_batch_size * 1.5) + 1,
                int(target_batch_time * item_per_unit_time + 0.5))
            desired_batch_size = min(max(1, desired_batch_size), max_batch_size)
            logging.debug("new batch size for %s is %d", peer, desired_batch_size)

    def _timeout_batch(self, batch):
        readd_list = []
        for (priority, block_hash, f, peers_tried) in batch:
            if not f.done():
                readd_list.append((priority, block_hash, f, peers_tried))
        if readd_list:
            logging.info("requeuing %d items starting with %s", len(readd_list), readd_list[0][0])
            for _ in readd_list:
                self._inv_item_future_queue.put_nowait(_)

    async def _event_worker(self):
        print("starting event worker")
        async for peer, message, data in self._event_aiter:
            if message != "block":
                continue
            block = data["block"]
            block_hash = block.hash()
            if block_hash in self._block_hash_to_future:
                f = self._block_hash_to_future[block_hash]
                if not f.done():
                    f.set_result((peer, block))
            else:
                logging.error("missing future for block %s from %s", block.id(), peer)


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
