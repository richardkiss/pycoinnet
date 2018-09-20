import asyncio
import logging
import weakref

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from .aitertools import map_filter_aiter


class BlockBatcher:
    def __init__(self, peer_manager):
        self._inv_item_future_queue = asyncio.PriorityQueue()
        self._block_hash_to_future = weakref.WeakValueDictionary()
        self._peer_aiter = peer_manager.new_peer_aiter()
        self._event_aiter = peer_manager.new_event_aiter()
        self._peer_task = asyncio.ensure_future(self._peer_worker())
        self._event_task = asyncio.ensure_future(self._event_worker())

    async def join(self):
        await self._peer_task
        await self._event_task

    def close(self):
        if not self._event_task.done():
            self._event_task.cancel()
        if not self._peer_task.done():
            self._peer_task.cancel()

    def __del__(self):
        self.close()

    async def _header_info_aiter_to_block_batcher_aiter(self, header_info):
        block_headers, first_block_index = header_info
        results = []
        for _, block_header in enumerate(block_headers):
            block_index = first_block_index + _
            results.append((block_index, await self._add_to_download_queue(block_header.hash(), block_index)))
        return results

    async def block_futures_for_header_info_aiter(self, header_info_aiter):
        async for peer, block_index, block_headers in header_info_aiter:
            for _, block_header in enumerate(block_headers):
                yield block_index + _, await self._add_to_download_queue(block_header.hash(), block_index + _)
        #await self._event_task

    async def _add_to_download_queue(self, block_hash, block_index):
        f = self._block_hash_to_future.get(block_hash)
        if not f:
            f = asyncio.Future()
            self._block_hash_to_future[block_hash] = f
            item = (block_index, block_hash, f, set())
            await self._inv_item_future_queue.put(item)
        return f

    async def _peer_worker(self):
        subtasks = set()
        async for peer in self._peer_aiter:
            NODE_NETWORK = 1
            if 1: #peer.version["services"] & NODE_NETWORK:
                # make two "fetch" tasks
                subtasks.update([asyncio.ensure_future(self._peer_batch_task(peer)) for _ in range(2)])
        if subtasks:
            done, pending = await asyncio.wait(subtasks)

    async def _get_batch(self, peer, desired_batch_size):
        batch = []
        skipped = []

        while True:
            if len(batch) > 0 and self._inv_item_future_queue.empty():
                break
            item = await self._inv_item_future_queue.get()
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
            await self._inv_item_future_queue.put(item)

        return batch

    async def _peer_batch_task(self, peer):
        loop = asyncio.get_event_loop()
        target_batch_time = 5.0
        desired_batch_size = 1
        max_batch_size = 100
        while True:
            get_batch_task = asyncio.ensure_future(self._get_batch(peer, desired_batch_size))
            done, pending = await asyncio.wait([
                get_batch_task, peer.wait_until_close()], return_when=asyncio.FIRST_COMPLETED)
            if get_batch_task not in done:
                get_batch_task.cancel()
                break
            batch = await get_batch_task
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
            # BRAIN DAMAGE: we shouldn't punish for retries since the original request may
            # finally respond            
            got_all = all(_.result()[0] == peer for _ in done)
            if not got_all:
                logging.info("peer %s didn't respond to all requests, sleeping for 60 s", peer)
                await asyncio.wait([peer.wait_until_close(), asyncio.sleep(60)], return_when=asyncio.FIRST_COMPLETED)
            logging.debug("new batch size for %s is %d", peer, desired_batch_size)
        logging.debug("ending _peer_batch_task for %s", peer)

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
        logging.debug("starting event worker")
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
        logging.debug("ending event worker")
