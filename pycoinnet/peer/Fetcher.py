import asyncio
import logging
import weakref

from pycoin.serialize import b2h_rev

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK


class Fetcher:
    """
    Fetching a merkleblock also fetches the transactions that follow, and
    includes them in the message as the "tx" key.
    """
    def __init__(self, peer):
        self.peer = peer
        self.request_q = asyncio.Queue()
        self.futures = weakref.WeakValueDictionary()

        getdata_loop_future = asyncio.Task(self._getdata_loop())
        next_message = peer.new_get_next_message_f(
            filter_f=lambda name, data: name in ["tx", "block", "merkleblock", "notfound"])
        peer.add_task(self._fetch_loop(next_message, getdata_loop_future))

    def fetch(self, inv_item, timeout=None):
        """
        Return the fetched object or None if the remote says it doesn't have it, or
        times out by exceeding `timeout` seconds.
        """
        future = self.futures.get(inv_item)
        if not future:
            future = asyncio.Future()
            self.futures[inv_item] = future
            self.request_q.put_nowait(inv_item)
        try:
            return (yield from asyncio.wait_for(future, timeout=timeout))
        except asyncio.TimeoutError:
            return None

    def queue_size(self):
        pass
        ## TODO: finish

    @asyncio.coroutine
    def _getdata_loop(self):
        while True:
            so_far = []
            inv_item = yield from self.request_q.get()
            while True:
                so_far.append(inv_item)
                if self.request_q.qsize() == 0 or len(so_far) >= 50000:
                    break
                inv_item = yield from self.request_q.get()
            self.peer.send_msg("getdata", items=so_far)

    @asyncio.coroutine
    def _fetch_loop(self, next_message, getdata_loop_future):
        try:
            while True:
                name, data = yield from next_message()
                ITEM_LOOKUP = dict(tx="tx", block="block", merkleblock="header")
                if name in ITEM_LOOKUP:
                    item = data[ITEM_LOOKUP[name]]
                    the_hash = item.hash()
                    TYPE_DB = { "tx" : ITEM_TYPE_TX, "block" : ITEM_TYPE_BLOCK, "merkleblock" : ITEM_TYPE_MERKLEBLOCK }
                    the_type = TYPE_DB[name]
                    inv_item = InvItem(the_type, the_hash)
                    if name == "merkleblock":
                        txs = []
                        for h in data["tx_hashes"]:
                            name, data = yield from next_message()
                            if name != "tx":
                                logging.error("insufficient tx messages after merkleblock message: missing %s", b2h_rev(h))
                                del self.futures[inv_item]
                                future.set_result(None)
                                break
                            tx = data["tx"]
                            if tx.hash() != h:
                                logging.error("missing tx message after merkleblock message: missing %s", b2h_rev(h))
                                del self.futures[inv_item]
                                future.set_result(None)
                                break
                            txs.append(tx)
                        item.txs = txs
                    future = self.futures.get(inv_item)
                    if future:
                        del self.futures[inv_item]
                        if not future.done():
                            future.set_result(item)
                        else:
                            logging.info("got %s unsolicited", item.id())
                if name == "notfound":
                    for inv_item in data["items"]:
                        the_hash = inv_item.data
                        future = self.futures.get(inv_item)
                        if future:
                            del self.futures[inv_item]
                            future.set_result(None)
        except EOFError:
            getdata_loop_future.cancel()
