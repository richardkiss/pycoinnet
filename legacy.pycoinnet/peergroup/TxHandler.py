"""
TxHandler

This class should be instantiated once per client.

It takes an InvCollector and a TxStore, and an optional Tx validator.

When a new Tx object is noted by the InvCollector, this object will
fetch it, validate it, then store it in the TxStore and tell the
InvCollector to advertise it to other peers.

When a new peer comes online, invoke add_peer.

This object will then watch for mempool and getdata messages
and handle them appropriately.
"""

import asyncio
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_TX


class TxHandler:
    def __init__(self, inv_collector, tx_store, tx_validator=lambda tx: True):
        self.inv_collector = inv_collector
        self.q = inv_collector.new_inv_item_queue()
        self.tx_store = tx_store
        self._validator_handle = asyncio.Task(self._run(tx_validator))

    def add_peer(self, peer):
        """
        Call this method when a peer comes online and you want to keep its mempool
        in sync with this mempool.
        """
        @asyncio.coroutine
        def _run_getdata(next_message):
            while True:
                name, data = yield from next_message()
                inv_items = data["items"]
                not_found = []
                txs_found = []
                for inv_item in inv_items:
                    if inv_item.item_type != ITEM_TYPE_TX:
                        continue
                    tx = self.tx_store.get(inv_item.data)
                    if tx:
                        txs_found.append(tx)
                    else:
                        not_found.append(inv_item)
                if not_found:
                    peer.send_msg("notfound", items=not_found)
                for tx in txs_found:
                    peer.send_msg("tx", tx=tx)

        @asyncio.coroutine
        def _run_mempool(next_message):
            try:
                name, data = yield from next_message()
                inv_items = [InvItem(ITEM_TYPE_TX, tx.hash()) for tx in self.tx_store.values()]
                logging.debug("sending inv of %d item(s) in response to mempool", len(inv_items))
                if len(inv_items) > 0:
                    peer.send_msg("inv", items=inv_items)
                # then we exit. We don't need to handle this message more than once.
            except EOFError:
                pass

        next_getdata = peer.new_get_next_message_f(lambda name, data: name == 'getdata')
        peer.add_task(_run_getdata(next_getdata))
        next_mempool = peer.new_get_next_message_f(lambda name, data: name == 'mempool')
        peer.add_task(_run_mempool(next_mempool))
        peer.send_msg("mempool")

    def add_tx(self, tx):
        """
        Add a transaction to the mempool and advertise it to peers so it can
        propogate throughout the network.
        """
        the_hash = tx.hash()
        if the_hash not in self.tx_store:
            self.tx_store[the_hash] = tx
            self.inv_collector.advertise_item(InvItem(ITEM_TYPE_TX, the_hash))

    @asyncio.coroutine
    def _run(self, tx_validator):
        while True:
            inv_item = yield from self.q.get()
            if inv_item.item_type != ITEM_TYPE_TX:
                continue
            self.inv_collector.fetch_validate_store_item_async(inv_item, self.tx_store, tx_validator)
