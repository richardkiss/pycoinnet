import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.inv_batcher import InvBatcher


def make_peer_to_header_tuples(peer_q, inv_batcher, blockchain_view, timeout):

    async def peer_to_header_tuples(peer, q):
        while True:
            block_locator_hashes = blockchain_view.block_locator_hashes()
            hash_stop = blockchain_view.hash_initial_block()
            logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])

            headers = []
            if not peer.is_closing():
                request = peer.request_response(
                    "getheaders", "headers", version=1,
                    hashes=block_locator_hashes, hash_stop=hash_stop)
                done, pending = await asyncio.wait([request], timeout=timeout)
                if done:
                    data = await done.pop()
                    headers = [bh for bh, t in data["headers"]]

            if len(headers) == 0:
                # this peer has exhausted its view
                # don't add it back to the queue
                break

            while len(headers) > 0 and headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1]:
                # this hack is necessary because the stupid default client
                # does not send the genesis block!
                bh = headers[0].previous_block_hash
                f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
                block = await f
                headers = [block] + headers

            block_number = blockchain_view.do_headers_improve_path(headers)
            if block_number is False:
                await peer_q.put(peer)
                break

            logging.debug("block header count is now %d", block_number)
            hashes = []

            for idx in range(blockchain_view.last_block_index()+1-block_number):
                the_tuple = blockchain_view.tuple_for_index(idx+block_number)
                assert the_tuple[0] == idx + block_number
                assert headers[idx].hash() == the_tuple[1]
                hashes.append(headers[idx])

            await q.put((block_number, hashes))

        if peer_q.qsize() == 0:
            await q.put(None)

    return peer_to_header_tuples


def make_create_block_hash_entry(inv_batcher, filter_f):
    async def create_block_hash_entry(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_headers = item
        logging.info("got %d new header(s) starting at %d" % (len(block_headers), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_headers)]

        for bh, block_index in block_hash_priority_pair_list:
            item_type = filter_f(bh, block_index)
            if item_type:
                f = await inv_batcher.inv_item_to_future(
                    InvItem(item_type, bh.hash()), priority=block_index)
            else:
                f = asyncio.Future()
                f.set_result(bh)
            await q.put((f, block_index))
    return create_block_hash_entry


async def header_to_block(next_item, q):
    if next_item is None:
        await q.put(None)
        return
    block_future, index = next_item
    block = await block_future
    if hasattr(block, "tx_futures"):
        txs = []
        for f in block.tx_futures:
            txs.append(await f)
        block.txs = txs
    await q.put((block, index))


def create_fetch_blocks_after_q(
        peer_manager, blockchain_view, filter_f=None, timeout=10.0, loop=None):
    # return a Queue that gets filled with blocks until we run out

    inv_batcher = InvBatcher(peer_manager)

    peer_q = peer_manager.new_peer_pipeline()
    peer_to_header_tuples = make_peer_to_header_tuples(peer_q, inv_batcher, blockchain_view, timeout)

    filter_f = filter_f or (lambda block_hash, index: ITEM_TYPE_BLOCK)

    create_block_hash_entry = make_create_block_hash_entry(inv_batcher, filter_f)

    peer_to_blocks = MappingQueue(
        dict(callback_f=peer_to_header_tuples, input_q=peer_q, worker_count=2),
        dict(callback_f=create_block_hash_entry, worker_count=1, input_q_maxsize=2),
        dict(callback_f=header_to_block, worker_count=1, input_q_maxsize=100)
    )

    return peer_to_blocks


def fetch_blocks_after(peer_manager, blockchain_view, filter_f=None):

    # yields blocks until we run out
    loop = asyncio.get_event_loop()

    block_index_q = create_fetch_blocks_after_q(peer_manager, blockchain_view, filter_f)

    while True:
        v = loop.run_until_complete(block_index_q.get())
        if v is None:
            break
        yield v
