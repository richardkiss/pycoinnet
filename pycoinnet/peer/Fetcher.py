import asyncio
import logging
import weakref

from pycoin.serialize import b2h_rev

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK


class Fetcher:
    """
    This class handles fetching transaction, block, or merkleblock objects
    advertised by an "inv" message. It automatically coalesces repeated requests
    for the same object.

    Fetching a merkleblock also fetches the transactions that follow, and
    includes them in the message as the "tx" key.
    """
    def __init__(self, peer, batch_size=50000):
        """ peer: a BitcoinPeerProtocol object """
        self.peer = peer
        self.request_q = asyncio.Queue()
        self.futures = weakref.WeakValueDictionary()
        self.batch_size = batch_size

        getdata_loop_future = asyncio.Task(self._getdata_loop())
        next_message = peer.new_get_next_message_f(
            filter_f=lambda name, data: name in ["tx", "block", "merkleblock", "notfound"])
        peer.add_task(self._fetch_loop(next_message, getdata_loop_future))

    @asyncio.coroutine
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

    def pending_response_count(self):
        """
        Return the number of items that have been requested to the remote
        that have not yet been answered.
        """
        return len(self.futures)

    def pending_request_count(self):
        """
        Return the number of items that are waiting to be requested to the remote.
        """
        return self.request_q.qsize()

    @asyncio.coroutine
    def _getdata_loop(self):
        """
        This is the task that sends a message to the peer requesting objects
        that have been requested. It coalesces requests for multiple objects
        into one message.
        """
        while True:
            so_far = []
            inv_item = yield from self.request_q.get()
            while inv_item is not None:
                so_far.append(inv_item)
                if self.request_q.qsize() == 0 or len(so_far) >= self.batch_size:
                    break
                inv_item = yield from self.request_q.get()
            self.peer.send_msg("getdata", items=so_far)
            if inv_item is None:
                break

    @asyncio.coroutine
    def _fetch_loop(self, next_message, getdata_loop_future):
        ITEM_LOOKUP = dict(tx="tx", block="block", merkleblock="header")
        TYPE_DB = dict(tx=ITEM_TYPE_TX, block=ITEM_TYPE_BLOCK,
                       merkleblock=ITEM_TYPE_MERKLEBLOCK)
        try:
            while True:
                name, data = yield from next_message()
                if name in ITEM_LOOKUP:
                    item = data[ITEM_LOOKUP[name]]
                    the_hash = item.hash()
                    the_type = TYPE_DB[name]
                    inv_item = InvItem(the_type, the_hash)
                    future = self.futures.get(inv_item)
                    if name == "merkleblock":
                        # we now expect a bunch of tx messages
                        txs = []
                        for h in data["tx_hashes"]:
                            name, data = yield from next_message()
                            if name != "tx":
                                logging.error(
                                    "insufficient tx messages after merkleblock message: missing %s",
                                    b2h_rev(h))
                                del self.futures[inv_item]
                                future.set_result(None)
                                break
                            tx = data["tx"]
                            if tx.hash() != h:
                                logging.error(
                                    "missing tx message after merkleblock message: missing %s", b2h_rev(h))
                                del self.futures[inv_item]
                                future.set_result(None)
                                break
                            txs.append(tx)
                        item.txs = txs
                    if future is not None:
                        del self.futures[inv_item]
                        if not future.done():
                            future.set_result(item)
                        else:
                            logging.info("got %s again %s", name, item.id())
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
