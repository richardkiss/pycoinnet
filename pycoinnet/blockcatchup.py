import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.MappingQueue import MappingQueue

from pycoinnet.inv_batcher import InvBatcher


def create_peer_to_block_pipe(bcv, filter_f=lambda block_hash, index: ITEM_TYPE_BLOCK):
    """
    return a MappingQueue that accepts: peer objects
    and yields: block objects
    """

    inv_batcher = InvBatcher()

    improve_headers_pipe = asyncio.Queue()

    async def note_peer(peer, q):
        await inv_batcher.add_peer(peer)
        await q.put(peer)

    async def improve_headers(peer, q):
        block_locator_hashes = bcv.block_locator_hashes()
        logging.debug("getting headers after %d", bcv.last_block_tuple()[0])
        data = await peer.request_response(
            "getheaders", "headers", version=1,
            hashes=block_locator_hashes, hash_stop=bcv.hash_initial_block())
        headers = [bh for bh, t in data["headers"]]

        if block_locator_hashes[-1] == bcv.hash_initial_block():
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
            block = await f
            headers = [block] + headers

        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            await q.put(None)
            return

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(bcv.last_block_index()+1-block_number):
            the_tuple = bcv.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])
        await q.put((block_number, hashes))
        await improve_headers_pipe.put(peer)

    async def create_block_hash_entry(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_headers = item
        logging.info("got %d new header(s) starting at %d" % (len(block_headers), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_headers)]

        for bh, pri in block_hash_priority_pair_list:
            item_type = filter_f(bh, pri)
            if not item_type:
                continue
            f = await inv_batcher.inv_item_to_future(InvItem(item_type, bh.hash()))
            await q.put((f, pri))

    async def wait_future(pair, q):
        if pair is None:
            await q.put(None)
            return
        future, index = pair
        block = await future
        await q.put((block, index))

    peer_to_block_pipe = MappingQueue(
        dict(callback_f=note_peer),
        dict(callback_f=improve_headers, worker_count=1, input_q=improve_headers_pipe),
        dict(callback_f=create_block_hash_entry, worker_count=1, input_q_maxsize=2),
        dict(callback_f=wait_future, input_q_maxsize=1000)
    )
    peer_to_block_pipe.inv_batcher = inv_batcher
    return peer_to_block_pipe
