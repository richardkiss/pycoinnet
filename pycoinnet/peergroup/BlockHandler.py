"""
BlockHandler

This class should be instantiated once per client.

It takes an InvCollector, a BlockChain, and a BlockStore.

It monitors both the BlockChain and the InvCollector.

When a new Block object is noted by the InvCollector, this object will
fetch it, validate it, then store it in the BlockStore and tell the
InvCollector to advertise it to other peers.

When the BlockChain notes a change, it checks the BlockStore to see
if it's available; if not, and should_download_f returns True, it
attempts to fetch it, validate it, then store it in the BlockStore
and tell the InvCollector to advertise it to other peers.

If a new block is mined, it should be added via the add_block
method of this class. It will then advertise it to peers so
it will propogate.

When a new peer comes online, invoke add_peer.

This object will then watch for getheaders, getblock and getdata messages
and handle them appropriately.
"""

from pycoinnet.util.debug_help import asyncio
import io
import logging

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK


# TODO: move to pycoin
def _header_for_block(block):
    from pycoin.block import BlockHeader
    f = io.BytesIO()
    block.stream(f)
    f.seek(0)
    return BlockHeader.parse(f)


class BlockHandler:
    def __init__(self, inv_collector, block_chain, block_store,
                 should_download_f=lambda block_hash, block_index: True,
                 block_validator=lambda block: True):
        self.inv_collector = inv_collector
        self.block_chain = block_chain
        self.block_store = block_store
        self.q = inv_collector.new_inv_item_queue()
        self._watch_invcollector_task = asyncio.Task(self._watch_invcollector(block_validator))
        # asyncio.Task(self._watch_block_chain(block_chain.new_change_q(), should_download_f))

    @asyncio.coroutine
    def _watch_block_chain(self, change_q, should_download_f):
        # this is only useful when fast-forwarding
        # we will skip it for now
        # TODO: implement
        def _download_block(block_hash, block_index):
            block = yield from self.inv_collector.fetch(InvItem(ITEM_TYPE_BLOCK, block_hash))
            return block
        while True:
            add_or_remove, block_hash, block_index = yield from change_q.get()
            if add_or_remove != "add":
                continue
            block = self.block_store.get(block_hash)
            if block:
                continue
            if should_download_f(block_hash, block_index):
                # BRAIN DAMAGE: we have to put the task somewhere smart
                self._download_task = asyncio.Task(_download_block(block_hash, block_index))

    def _prep_headers(self, hash_stop):
        headers = []
        if hash_stop == b'\0' * 32:
            for i in range(min(2000, self.block_chain.length())):
                h = self.block_chain.hash_for_index(i)
                if h is None:
                    break
                block = self.block_store.get(h)
                if block is None:
                    break
                headers.append((_header_for_block(block), 0))
        # ## TODO: handle other case where hash_stop != b'\0' * 32
        return headers

    def add_peer(self, peer):
        """
        Call this method when a peer comes online.
        """
        @asyncio.coroutine
        def _run_handle_get(next_message):
            while True:
                name, data = yield from next_message()
                if name == 'getblocks':
                    continue
                    # TODO: get this working
                    # block_hashes = _prep_block_hashes(data.get("hash_stop"))
                    # if block_hashes:
                    #    peer.send_msg("headers", headers=block_hashes)
                if name == 'getheaders':
                    headers = self._prep_headers(data.get("hash_stop"))
                    if headers:
                        logging.debug("sending %d headers", len(headers))
                        peer.send_msg("headers", headers=headers)
                if name == 'getdata':
                    inv_items = data["items"]
                    not_found = []
                    blocks_found = []
                    for inv_item in inv_items:
                        if inv_item.item_type != ITEM_TYPE_BLOCK:
                            continue
                        block = self.block_store.get(inv_item.data)
                        if block:
                            blocks_found.append(block)
                        else:
                            not_found.append(inv_item)
                    if not_found:
                        logging.debug("could not find %d blocks", len(not_found))
                        peer.send_msg("notfound", items=not_found)
                    logging.debug("sending %d blocks", len(blocks_found))
                    for block in blocks_found:
                        logging.debug("sending block %s", block.id())
                        peer.send_msg("block", block=block)

        next_message = peer.new_get_next_message_f(
            lambda name, data: name in ['getheaders', 'getblocks', 'getdata'])
        peer.add_task(_run_handle_get(next_message))

    def add_block(self, block):
        """
        Add a block and advertise it to peers so it can propogate throughout the network.
        """
        the_hash = block.hash()
        if the_hash not in self.block_store:
            self.block_store[the_hash] = block
            self.inv_collector.advertise_item(InvItem(ITEM_TYPE_BLOCK, the_hash))

    @asyncio.coroutine
    def _watch_invcollector(self, block_validator):
        while True:
            inv_item = yield from self.q.get()
            if inv_item.item_type != ITEM_TYPE_BLOCK:
                continue
            self.inv_collector.fetch_validate_store_item_async(inv_item, self.block_store, block_validator)
