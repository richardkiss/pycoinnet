import asyncio
import logging
import weakref

from pycoin.message.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK
from pycoinnet.MappingQueue import MappingQueue


class InvBatcher:
    def __init__(self, target_batch_time=10, max_batch_time=30, max_batch_size=500, inv_item_future_q_maxsize=1000):

        self._is_closing = False
        self._inv_item_future_queue = asyncio.PriorityQueue(maxsize=inv_item_future_q_maxsize)

        async def batch_getdata_fetches(peer_batch_tuple, q):
            peer, desired_batch_size = peer_batch_tuple
            batch = []
            skipped = []
            logging.debug("peer %s building batch max size %d", peer, desired_batch_size)
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

            complete_count = 0
            while True:
                last_complete_count = complete_count
                await asyncio.wait(futures, timeout=target_batch_time)
                batch_time = loop.time() - start_time

                complete_count = sum([1 for _ in futures if _.done()])
                if complete_count == len(futures):
                    break

                if last_complete_count >= complete_count or batch_time > max_batch_time:
                    break

            for (priority, inv_item, f, peers_tried) in batch:
                if not f.done():
                    peers_tried.add(peer)
                    await self._inv_item_future_queue.put((priority, inv_item, f, peers_tried))

            logging.debug("got %d of %d batch items in %f s from %s", complete_count,
                          len(inv_items), batch_time, peer)

            if peer.is_closing():
                logging.debug("peer closing %s", peer)
                return

            item_per_unit_time = complete_count / batch_time
            new_batch_size = min(prior_max * 4, int(target_batch_time * item_per_unit_time + 0.5))
            new_batch_size = min(max(1, new_batch_size), max_batch_size)
            logging.debug("new batch size for %s is %d", peer, new_batch_size)
            await self._peer_batch_queue.put((peer, new_batch_size))

        self._peer_batch_queue = MappingQueue(
            dict(callback_f=batch_getdata_fetches),
            dict(callback_f=fetch_batch, input_q_maxsize=2, worker_count=2),
        )

        self._inv_item_hash_to_future = weakref.WeakValueDictionary()

    async def inv_item_to_future(self, inv_item, priority=0):
        f = self._inv_item_hash_to_future.get(inv_item)
        if f is None:
            f = self.register_interest(inv_item)
            item = (priority, inv_item, f, set())
            await self._inv_item_future_queue.put(item)
        return f

    async def add_peer(self, peer, initial_batch_size=1):
        self.register_peer_callbacks(peer)
        await self._peer_batch_queue.put((peer, initial_batch_size))
        await self._peer_batch_queue.put((peer, initial_batch_size))

    def register_peer_callbacks(self, peer):
        peer.set_request_callback("block", self.handle_block_event)
        peer.set_request_callback("merkleblock", self.handle_merkle_block_event)
        peer.set_request_callback("tx", self.handle_tx_event)

    def stop(self):
        self._peer_batch_queue.stop()

    def register_interest(self, inv_item):
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
        return f

    def _handle_inv_response(self, peer, item_type, item):
        item_hash = item.hash()
        inv_item = InvItem(item_type, item_hash)
        if inv_item in self._inv_item_hash_to_future:
            f = self._inv_item_hash_to_future[inv_item]
            if not f.done():
                f.set_result(item)
        else:
            logging.error("missing future for item %s from %s", item.id(), peer)

    def handle_block_event(self, peer, name, data):
        item = data["block" if name == "block" else "header"]
        self._handle_inv_response(peer, ITEM_TYPE_BLOCK, item)

    def handle_merkle_block_event(self, peer, name, data):
        item = data["header"]
        item.tx_futures = [self.register_interest(InvItem(ITEM_TYPE_TX, tx_hash)) for tx_hash in data["tx_hashes"]]
        self._handle_inv_response(peer, ITEM_TYPE_MERKLEBLOCK, item)

    def handle_tx_event(self, peer, name, data):
        item = data["tx"]
        self._handle_inv_response(peer, ITEM_TYPE_TX, item)
