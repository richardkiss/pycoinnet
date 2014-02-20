import asyncio
import weakref

from pycoinnet.InvItem import InvItem

class Fetcher:
    def __init__(self, peer, item_id):
        self.peer = peer
        self.request_q = asyncio.Queue()
        self.futures = weakref.WeakValueDictionary()

        getdata_loop_future = asyncio.Task(self._getdata_loop(item_id))
        msg = [0, "tx", "block"][item_id]
        next_message = peer.new_get_next_message_f(filter_f=lambda name, data: name in [msg, "notfound"])
        asyncio.Task(self._fetch_loop(msg, next_message, getdata_loop_future))

    def fetch(self, the_hash, timeout=None):
        """
        Return the fetched object or None if the remote says it doesn't have it, or
        times out by exceeding `timeout` seconds.
        """
        future = self.fetch_future(the_hash)
        try:
            return (yield from asyncio.wait_for(future, timeout=timeout))
        except asyncio.Timeout:
            return None

    def fetch_future(self, the_hash):
        self.request_q.put_nowait(the_hash)
        future = self.futures.get(the_hash)
        if not future:
            future = asyncio.Future()
            self.futures[the_hash] = future
        return future

    def queue_size(self):
        pass
        ## TODO: finish

    @asyncio.coroutine
    def _getdata_loop(self, item_id):
        while True:
            so_far = []
            the_hash = yield from self.request_q.get()
            while True:
                so_far.append(InvItem(item_id, the_hash))
                if self.request_q.qsize() == 0 or len(so_far) >= 50000:
                    break
                the_hash = yield from self.request_q.get()
            self.peer.send_msg("getdata", items=so_far)

    def _fetch_loop(self, msg, next_message, getdata_loop_future):
        try:
            while True:
                name, data = yield from next_message()
                if name == msg:
                    item = data[msg]
                    the_hash = item.hash()
                    future = self.futures.get(the_hash)
                    if future:
                        del self.futures[the_hash]
                        if not future.done():
                            future.set_result(item)
                        else:
                            logging.info("got %s unsolicited", item.id())
                if name == "notfound":
                    for inv_item in data["items"]:
                        the_hash = inv_item.data
                        future = self.futures.get(the_hash)
                        if future:
                            del self.futures[the_hash]
                            future.set_result(None)
        except EOFError:
            getdata_loop_future.cancel()
