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

from pycoinnet.peer.Fetcher import Fetcher


class InvCollector:
    def __init__(self, tx_store={}, block_store={}):
        self.inv_item_db = {}
        # key: InvItem; value: dictionary of peers to timestamps

        self.fetchers_by_peer = {}
        self.advertise_queues = weakref.WeakSet()
        self.inv_item_queues = weakref.WeakSet()
        self.inv_item_peers_q = {}

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
                    if name == 'inv':
                        for inv_item in data["items"]:
                            logging.debug("noting %s available from %s", inv_item, peer)
                            self._register_inv_item(inv_item, peer)
                    if name == 'notfound':
                        for inv_item in data["items"]:
                            logging.debug("noting %s not available from %s", inv_item, peer)
                            self._unregister_inv_item(inv_item, peer)
            except EOFError:
                del self.fetchers_by_peer[peer]
                advertise_task.cancel()
                for q in self.inv_item_queues:
                    q.put_nowait(None)

        advertise_task = asyncio.Task(_advertise_to_peer(peer, q))

        next_message = peer.new_get_next_message_f(lambda name, data: name in ("inv", "notfound"))
        peer.add_task(_watch_peer(peer, next_message, advertise_task))

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
        # create the queue of peers that have this inv_item available
        q = asyncio.Queue()
        items = sorted(self.inv_item_db[inv_item.data].items(), key=lambda pair: pair[-1])
        for peer, when in items:
            q.put_nowait(peer)
        # make the queue available to the object so if more peers
        # announce they have it, they can be queried
        self.inv_item_peers_q[inv_item.data] = q

        # this is the set of futures that we are trying to fetch the item from
        pending_fetchers = set()

        # this async method delays a specific amount of time, then
        # fetches a new peer from the queue
        @asyncio.coroutine
        def _wait_for_timeout_and_peer(q, initial_delay=0):
            yield from asyncio.sleep(initial_delay)
            while True:
                peer = yield from q.get()
                fetcher = self.fetchers_by_peer.get(peer)
                if fetcher:
                    break
            logging.debug("requesting %s from %s", inv_item, peer)
            return asyncio.Task(fetcher.fetch(inv_item))

        # the loop works like this:
        #   request the item from a peer
        #   wait 10 s or for response (either notfound or found)
        #   if found, done
        #   if notfound or time out, get another peer

        most_recent_fetcher = None

        while True:
            if most_recent_fetcher is None and q.qsize() > 0:
                most_recent_fetcher = yield from _wait_for_timeout_and_peer(q)
                timer_future = asyncio.Task(_wait_for_timeout_and_peer(q, initial_delay=peer_timeout))

            futures = pending_fetchers.union(set([timer_future]))

            if most_recent_fetcher:
                futures.add(most_recent_fetcher)

            done, pending_fetchers = \
                yield from asyncio.wait(list(futures), return_when=asyncio.FIRST_COMPLETED)

            # is it time to try a new fetcher?
            if timer_future in done:
                # we timed out, so we need to queue up another peer
                most_recent_fetcher = timer_future.result()
                logging.debug("timeout, need to request from a new peer, %s", inv_item)
                timer_future = asyncio.Task(_wait_for_timeout_and_peer(q, initial_delay=peer_timeout))
                # we have a new peer available as the result of get_fetcher_future
                continue

            # is the most recent done?
            if most_recent_fetcher and most_recent_fetcher.done():
                r = most_recent_fetcher.result()
                if r:
                    return r
                # we got a "notfound" from this one
                # queue up another peer
                logging.debug("got a notfound, need to try a new peer for %s", inv_item)
                timer_future.cancel()
                pending_fetchers.discard(timer_future)
                most_recent_fetcher = None
                timer_future = asyncio.Task(_wait_for_timeout_and_peer(q, initial_delay=0))
                # we have a new peer available as the result of timer_future
                continue

            # one or more fetchers is done
            # if any of them have a non-None result, we're golden
            for f in done:
                r = f.result()
                if r:
                    logging.info("Got %s", r)
                    return r
            # otherwise, we just continue trying, using the new pending_fetchers

    def fetch_validate_store_item_async(self, inv_item, item_store, validator_f):
        def _run():
            item = item_store.get(inv_item.data)
            if item:
                return
            item = yield from self.fetch(inv_item)
            if item and validator_f(item):
                item_store[item.hash()] = item
                self.advertise_item(inv_item)
        return asyncio.Task(_run())

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
        if the_hash in self.inv_item_peers_q:
            self.inv_item_peers_q[the_hash].put_nowait(peer)

    def _unregister_inv_item(self, inv_item, peer):
        the_hash = inv_item.data
        if the_hash in self.inv_item_db:
            del self.inv_item_db[the_hash][peer]
