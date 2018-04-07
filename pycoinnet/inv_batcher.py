import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK
from pycoinnet.MappingQueue import MappingQueue


class InvBatcher:
    def __init__(self, target_batch_time=10, max_batch_size=500, inv_item_future_q_maxsize=1000):

        self._is_closing = False
        self._inv_item_future_queue = asyncio.PriorityQueue(maxsize=inv_item_future_q_maxsize)

        async def batch_getdata_fetches(peer_batch_tuple, q):
            peer, desired_batch_size = peer_batch_tuple
            batch = []
            skipped = []
            logging.info("peer %s trying to build batch up to size %d", peer, desired_batch_size)
            while len(batch) == 0 or (
                    len(batch) < desired_batch_size and not self._inv_item_future_queue.empty()):
                item = await self._inv_item_future_queue.get()
                (priority, inv_item, f, peers_tried) = item
                if f.done():
                    continue
                if peer in peers_tried:
                    skipped.append(item)
                else:
                    batch.append(item)
            if len(batch) > 0:
                await q.put((peer, batch, desired_batch_size))
            for item in skipped:
                if not item[2].done:
                    await self._inv_item_future_queue.put(item)

        async def fetch_batch(peer_batch, q):
            loop = asyncio.get_event_loop()
            peer, batch, prior_max = peer_batch
            inv_items = [inv_item for (priority, inv_item, f, peers_tried) in batch]
            peer.send_msg("getdata", items=inv_items)
            start_time = loop.time()
            futures = [f for (priority, bh, f, peers_tried) in batch]
            await asyncio.wait(futures, timeout=target_batch_time)
            end_time = loop.time()
            batch_time = end_time - start_time
            logging.info("completed batch size of %d with time %f", len(inv_items), batch_time)
            completed_count = sum([1 for f in futures if f.done()])
            item_per_unit_time = completed_count / batch_time
            new_batch_size = min(prior_max * 4, int(target_batch_time * item_per_unit_time + 0.5))
            new_batch_size = min(max(1, new_batch_size), max_batch_size)
            logging.info("new batch size for %s is %d", peer, new_batch_size)
            for (priority, inv_item, f, peers_tried) in batch:
                if not f.done():
                    peers_tried.add(peer)
                    await self._inv_item_future_queue.put((priority, inv_item, f, peers_tried))
            await self._peer_batch_queue.put((peer, new_batch_size))

        self._peer_batch_queue = MappingQueue(
            dict(callback_f=batch_getdata_fetches),
            dict(callback_f=fetch_batch, input_q_maxsize=2, worker_count=2),
        )

        self._inv_item_hash_to_future = dict()

    async def add_peer(self, peer, initial_batch_size=1):
        peer.set_request_callback("block", self.handle_block_event)
        peer.set_request_callback("merkleblock", self.handle_block_event)
        await self._peer_batch_queue.put((peer, initial_batch_size))
        await self._peer_batch_queue.put((peer, initial_batch_size))

    async def inv_item_to_future(self, inv_item, priority=0):
        f = self._inv_item_hash_to_future.get(inv_item)
        if not f:
            f = asyncio.Future()
            self._inv_item_hash_to_future[inv_item] = f

            def remove_later(f):

                def remove():
                    if inv_item in self._inv_item_hash_to_future:
                        del self._inv_item_hash_to_future[inv_item]

                asyncio.get_event_loop().call_later(5, remove)

            f.add_done_callback(remove_later)
            item = (priority, inv_item, f, set())
            await self._inv_item_future_queue.put(item)
        return f

    def _handle_inv_response(self, item_type, item):
        item_hash = item.hash()
        inv_item = InvItem(item_type, item_hash)
        if inv_item in self._inv_item_hash_to_future:
            f = self._inv_item_hash_to_future[inv_item]
            if not f.done():
                f.set_result(item)
        else:
            logging.error("missing future for item %s", item.id())

    def handle_block_event(self, peer, name, data):
        item = data["block" if name == "block" else "header"]
        if name == "merkleblock":
            item.tx_hashes = data["tx_hashes"]
        self._handle_inv_response(ITEM_TYPE_BLOCK if name == "block" else ITEM_TYPE_MERKLEBLOCK, item)

    def handle_tx_event(self, peer, name, data):
        item = data["tx"]
        self._handle_inv_response(ITEM_TYPE_TX, item)

    def stop(self):
        self._peer_batch_queue.stop()
