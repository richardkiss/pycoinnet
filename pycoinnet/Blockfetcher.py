
import asyncio
import collections
import logging
import weakref

from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_BLOCK


class Blockfetcher:
    """
    Blockfetcher

    This class parallelizes block fetching.
    When a new peer is connected, pass it in to add_peer
    and forward all messages of type "block" to handle_msg.

    To download a list of blocks, call "fetch_blocks".

    It accepts new peers via add_peer.

    It fetches new blocks via get_block_future or get_block.
    """
    def __init__(self, max_batch_size=300, initial_batch_size=1, target_batch_time=2, max_batch_timeout=7):
        # this queue accepts tuples of the form:
        #  (priority, InvItem(ITEM_TYPE_BLOCK, block_hash), future, peers_tried)
        self._block_hash_priority_queue = asyncio.PriorityQueue()
        self._retry_priority_queue = collections.deque()
        self._get_batch_lock = asyncio.Lock()
        self._futures = weakref.WeakValueDictionary()
        self._max_batch_size = max_batch_size
        self._initial_batch_size = initial_batch_size
        self._target_batch_time = target_batch_time
        self._max_batch_timeout = max_batch_timeout
        self._peer_locks = dict()

    def fetch_blocks(self, block_hash_priority_pair_list):
        """
        block_hash_priority_pair_list is a list of
        tuples with (block_hash, priority).
        The priority is generally expected block index.
        Blocks are prioritized by this priority.

        Returns: a list of futures, each corresponding to a tuple.
        """
        r = []
        if self._block_hash_priority_queue.empty():
            self._kick_all_peers()
        for bh, pri in block_hash_priority_pair_list:
            f = asyncio.Future()
            peers_tried = set()
            item = (pri, bh, f, peers_tried)
            self._block_hash_priority_queue.put_nowait(item)
            r.append(f)
            self._futures[bh] = f
        return r

    def fetch_block(self, priority, block_hash, peer):
        """
        This fetches a block from a SPECIFIC peer. It's designed to respond
        to an inv message.

        Returns: a future for the block.
        """
        # request the block from the given peer
        # put it in the retry queue
        if self._block_hash_priority_queue.empty():
            self._kick_all_peers()
        f = asyncio.Future()
        item = (priority, block_hash, f, set([peer]))
        self._futures[block_hash] = f
        now = asyncio.get_event_loop().time()
        self._retry_priority_queue.append((now + self._max_batch_timeout, [item]))
        peer.send_msg("getdata", items=InvItem(ITEM_TYPE_BLOCK, block_hash))
        return f

    def add_peer(self, peer):
        """
        Register a new peer, and start the loop which polls it for blocks.
        """
        self._peer_locks[peer] = asyncio.Lock()
        self._kick_peer(peer)

    def _kick_peer(self, peer):
        lock = self._peer_locks[peer]
        if not lock.locked():
            asyncio.get_event_loop().create_task(self._fetcher_loop(peer))

    def _kick_all_peers(self):
        for peer in self._peer_locks.keys():
            self._kick_peer(peer)

    def handle_msg(self, name, data):
        """
        When a peer gets a block message, it should invoked this method.
        """
        if name == 'block':
            block = data.get("block")
            bh = block.hash()
            f = self._futures.get(bh)
            if f and not f.done():
                f.set_result(block)
                del self._futures[bh]

    def _check_retry_q(self):
        """
        Deal with retry queue.
        Returns amount of time until first element in retry queue
        becomes valid, or None if there aren't any.
        """
        now = asyncio.get_event_loop().time()
        while len(self._retry_priority_queue) > 0:
            retry_time, items = self._retry_priority_queue[0]
            if retry_time > now:
                break
            self._retry_priority_queue.popleft()
            if self._block_hash_priority_queue.empty():
                self._kick_all_peers()
            first_pri = None
            last_pri = None
            for item in items:
                (pri, block_hash, block_future, peers_tried) = item
                if block_future.done():
                    continue
                if first_pri is None:
                    first_pri = pri
                last_pri = pri
                self._block_hash_priority_queue.put_nowait(item)
            if first_pri is not None:
                if first_pri == last_pri:
                    logging.info("timeout, retrying block %s", first_pri)
                else:
                    logging.info("timeout, retrying blocks %s - %s", first_pri, last_pri)

    @asyncio.coroutine
    def _get_batch(self, batch_size, peer):
        """
        Returns a batch of size "batch_size" of blocks to fetch that
        the given peer has not yet tried. Puts the batch into the retry queue,
        marked as tried with this peer.
        """
        with (yield from self._get_batch_lock):
            logging.info("getting batch up to size %d for %s", batch_size, peer)
            self._check_retry_q()

            # build a batch
            skipped = []
            items = []
            inv_items = []
            futures = []
            while len(futures) < batch_size:
                if self._block_hash_priority_queue.empty():
                    break
                item = yield from self._block_hash_priority_queue.get()
                (pri, block_hash, block_future, peers_tried) = item
                if block_future.done():
                    continue
                if peer in peers_tried:
                    skipped.append(item)
                    continue
                peers_tried.add(peer)
                inv_items.append(InvItem(ITEM_TYPE_BLOCK, block_hash))
                futures.append(block_future)
                items.append(item)
            now = asyncio.get_event_loop().time()
            self._retry_priority_queue.append((now + self._max_batch_timeout, items))
            for item in skipped:
                self._block_hash_priority_queue.put_nowait(item)
            if skipped:
                logging.info("some blocks in range %s-%s already tried by peer %s, skipping",
                             skipped[0][0], skipped[-1][0], peer)
            logging.info("returning batch of size %d for %s", len(futures), peer)
        start_batch_time = asyncio.get_event_loop().time()
        peer.send_msg("getdata", items=inv_items)
        logging.debug("requested %s from %s", [item[0] for item in items], peer)
        return futures, start_batch_time

    @asyncio.coroutine
    def _fetcher_loop(self, peer):
        """
        This is invoked as a task, for each peer. It grabs two batches,
        and starts downloading both of them. When the first batch finishes,
        it grabs another, so two overlapping batches are always being
        downloaded simultaneously.
        """
        lock = self._peer_locks[peer]
        if lock.locked():
            return
        with (yield from lock):
            batch_size = self._initial_batch_size
            loop = asyncio.get_event_loop()
            try:
                batch_1, start_batch_time_1 = yield from self._get_batch(batch_size=batch_size, peer=peer)
                while len(batch_1) > 0:
                    batch_2, start_batch_time_2 = yield from self._get_batch(
                        batch_size=batch_size, peer=peer)
                    yield from asyncio.wait(batch_1, timeout=self._max_batch_timeout)
                    # see how many items we got
                    item_count = sum(1 for f in batch_1 if f.done())
                    # calculate new batch size
                    batch_time = loop.time() - start_batch_time_1
                    logging.info("got %d items from batch size %d in %s s",
                                 item_count, len(batch_1), batch_time)
                    time_per_item = batch_time / max(1, item_count)
                    batch_size = min(int(self._target_batch_time / time_per_item) + 1, self._max_batch_size)
                    batch_1 = batch_2
                    logging.info("new batch size is %d", batch_size)
                    start_batch_time_1 = start_batch_time_2
            except EOFError:
                logging.info("peer %s disconnected", peer)
            except Exception:
                logging.exception("problem with peer %s", peer)
