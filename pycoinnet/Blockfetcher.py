
# this provides the following API:

# add_fetcher(inv_fetcher)
# get_block(block_hash, priority)

# get_blocks(hash_list, priority)


import asyncio

from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK


class Blockfetcher:
    """
    Blockfetcher

    This class is created once for each PeerGroup. It's used to accept a set of blocks
    and download them in an overlapping way.

    It accepts new peers via add_peer.

    It fetches new blocks via get_block_future or get_block.
    """
    def __init__(self, max_q_size=0):
        # this queue accepts tuples of the form:
        #  (priority, InvItem(ITEM_TYPE_BLOCK, block_hash), future)
        self._block_hash_priority_queue = asyncio.PriorityQueue(max_q_size)
        self._pending_futures = set()
        self._fetchers = []
        self._block_fetch_lock = asyncio.Lock()

    def add_fetcher(self, inv_fetcher):
        self._fetchers.append(inv_fetcher)

    def _kick(self):
        if self._block_hash_priority_queue.empty():
            asyncio.get_event_loop().create_task(self._block_fetch_loop())

    def get_block_future(self, block_hash, priority):
        future = asyncio.Future()
        item = (priority, InvItem(ITEM_TYPE_BLOCK, block_hash), future)
        self._kick()
        self._block_hash_priority_queue.put_nowait(item)
        return future

    def get_merkle_block_future(self, block_hash, priority):
        future = asyncio.Future()
        item = (priority, InvItem(ITEM_TYPE_MERKLEBLOCK, block_hash), future)
        self._kick()
        self._block_hash_priority_queue.put_nowait(item)
        return future

    @asyncio.coroutine
    def get_block(self, block_hash, priority):
        future = self.get_block_future(block_hash, priority)
        block = asyncio.wait_for(future, timeout=None)
        return block

    def _blocks_per_batch(self, fetcher):
        return 50

    @asyncio.coroutine
    def _fill_fetcher_queue(self, fetcher, timeout):
        items_to_skip = []
        blocks_per_batch = self._blocks_per_batch(fetcher)
        prc = fetcher.pending_response_count()
        while prc < blocks_per_batch:
            if self._block_hash_priority_queue.empty():
                break
            prc += 1
            item = yield from self._block_hash_priority_queue.get()
            # we need this line to update pending_response_count
            f = asyncio.ensure_future(fetcher.fetch(item[1], timeout=timeout))
            if hasattr(f, "item"):
                # we've tried this one already
                # let someone else have a crack
                items_to_skip.add(item)
                continue
            f.item = item
            f.fetcher = fetcher
            self._pending_futures.add(f)
        for item in items_to_skip:
            self._block_hash_priority_queue.put_nowait(item)

    @asyncio.coroutine
    def _block_fetch_loop(self):
        with (yield from self._block_fetch_lock):
            TIMEOUT = 10
            fetchers_to_queue = set(self._fetchers)
            while True:
                for fetcher in fetchers_to_queue:
                    yield from self._fill_fetcher_queue(fetcher, TIMEOUT)
                if len(self._pending_futures) == 0:
                    break
                done, pending = yield from asyncio.wait(
                    self._pending_futures, return_when=asyncio.FIRST_COMPLETED)
                fetchers_to_queue = set()
                for f in done:
                    try:
                        r = f.result()
                        related_future = f.item[-1]
                        if not related_future.done():
                            related_future.set_result(r)
                    except Exception:
                        yield from self._block_hash_priority_queue.put(f.item)
                    self._pending_futures.discard(f)
                    fetchers_to_queue.add(f.fetcher)
