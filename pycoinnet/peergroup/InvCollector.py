"""
InvCollector.py

Listen to peers and queue when new InvItem objects are seen.

Allow them to be fetched.

Advertise objects that are fetched (to peers that haven't told us they have it).
"""

import asyncio
import logging
import time
import weakref

from pycoinnet.InvItem import ITEM_TYPE_TX
from pycoinnet.peer.Fetcher import Fetcher


class InvCollector:
    def __init__(self, tx_store={}, block_store={}):
        self.inv_item_db = {}
        # key: InvItem; value: dictionary of peers to timestamps

        self.fetchers_by_peer = {}
        self.advertise_queues = weakref.WeakSet()
        self.inv_item_queues = set()
        self.inv_item_fetchers_q = {}

    def add_peer(self, peer):
        """
        Add a peer whose inv messages we want to monitor.
        """
        self.fetchers_by_peer[peer] = Fetcher(peer)
        q = asyncio.Queue()
        self.advertise_queues.add(q)

        @asyncio.coroutine
        def _advertise_to_peer(peer, q):
            while True:
                items = []
                while True:
                    inv_item = yield from q.get()
                    if peer not in self.inv_item_db.get(inv_item.data, []):
                        items.append(inv_item)
                    if q.qsize() == 0:
                        break
                # advertise the presence of the item!
                if len(items) > 0:
                    peer.send_msg("inv", items=items)

        @asyncio.coroutine
        def _watch_peer(peer, next_message, advertise_task):
            try:
                while True:
                    name, data = yield from next_message()
                    for inv_item in data["items"]:
                        logging.debug("noting %s available from %s", inv_item, peer)
                        self._register_inv_item(inv_item, peer)
            except EOFError:
                del self.fetchers_by_peer[peer]
                advertise_task.cancel()
                for q in self.inv_item_queues:
                    q.put_nowait(None)

        advertise_task = asyncio.Task(_advertise_to_peer(peer, q))

        next_message = peer.new_get_next_message_f(lambda name, data: name == "inv")
        asyncio.Task(_watch_peer(peer, next_message, advertise_task))

    def fetcher_for_peer(self, peer):
        return self.fetchers_by_peer.get(peer)

    def new_inv_item_queue(self):
        """
        Return a new queue that gets inv_items queued to it the first time
        they are seen. Invoke "get" to get them.
        """
        q = asyncio.Queue()
        self.inv_item_queues.add(q)
        return q

    @asyncio.coroutine
    def fetch(self, inv_item, peer_timeout=10):
        q = asyncio.Queue()
        items = sorted(self.inv_item_db[inv_item.data].items(), key=lambda pair: pair[-1])
        for peer, when in items:
            fetcher = self.fetchers_by_peer.get(peer)
            if fetcher:
                q.put_nowait((peer, fetcher))
        self.inv_item_fetchers_q[inv_item.data] = q

        futures = []

        @asyncio.coroutine
        def _q_change(q, futures, timeout=0):
            yield from asyncio.sleep(timeout)
            peer, fetcher = yield from q.get()
            logging.debug("requesting %s from %s", inv_item, peer)
            future = asyncio.Task(fetcher.fetch(inv_item))
            futures.append(future)

        while True:
            timeout = peer_timeout if len(futures) > 0 else 0
            q_change_future = asyncio.Task(_q_change(q, futures, timeout=timeout))
            all_futures = futures + [q_change_future]
            done, pending = yield from asyncio.wait(all_futures, return_when=asyncio.FIRST_COMPLETED)

            if q_change_future in done:
                continue

            for f in done:
                r = f.result()
                if r is None:
                    futures.remove(f)
                    del self.inv_item_db[inv_item.data][peer]
                else:
                    for p in pending:
                        p.cancel()
                    if inv_item.data in self.inv_item_fetchers_q:
                        del self.inv_item_fetchers_q[inv_item.data]
                    return r

            logging.debug("got notfound")

            if not q_change_future.cancelled():
                q_change_future.cancel()


    def advertise_item(self, inv_item):
        """
        Advertise an item to peers. Note that peers who have mentioned they have
        the item won't be advertised to.
        """
        for q in self.advertise_queues:
            q.put_nowait(inv_item)

    def _register_inv_item(self, inv_item, peer):
        the_hash = inv_item.data
        if the_hash not in self.inv_item_db:
            # it's new!
            self.inv_item_db[the_hash] = {}
            for q in self.inv_item_queues:
                q.put_nowait(inv_item)
        self.inv_item_db[the_hash][peer] = time.time()
        if the_hash in self.inv_item_fetchers_q:
            fetcher = self.fetchers_by_peer.get(peer)
            self.inv_item_fetchers_q[the_hash].put_nowait((peer, fetcher))
