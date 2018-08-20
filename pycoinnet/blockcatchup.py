import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.inv_batcher import InvBatcher


def create_peer_to_header_q(peer_manager, blockchain_view, inv_batcher, timeout=10.0, loop=None):
    # create and return a Mapping Queue that takes peers as input
    # and produces tuples of (initial_block, [headers]) as output

    peer_q = peer_manager.new_peer_pipeline()

    async def peer_to_header_tuples(peer, q):
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
            if peer_q.qsize() == 0:
                await q.put(None)
                # this peer has exhausted its view
            return

        while len(headers) > 0 and headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1]:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
            block = await f
            headers = [block] + headers

        block_number = blockchain_view.do_headers_improve_path(headers)
        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(blockchain_view.last_block_index()+1-block_number):
            the_tuple = blockchain_view.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])
        await q.put((block_number, hashes))
        await peer_q.put(peer)

    return MappingQueue(
        dict(callback_f=peer_to_header_tuples, input_q=peer_q, worker_count=1),
        final_q=asyncio.Queue(maxsize=2), loop=loop)


def create_header_to_block_future_q(inv_batcher, input_q=None, filter_f=None, loop=None):

    input_q = input_q or asyncio.Queue()
    filter_f = filter_f or (lambda block_hash, index: ITEM_TYPE_BLOCK)

    # accepts (initial_block_index, [headers]) tuples as input
    # produces (block_future, index) tuples as output

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

    return MappingQueue(
        dict(callback_f=create_block_hash_entry, input_q=input_q, worker_count=1),
        final_q=asyncio.Queue(maxsize=500), loop=loop)


def create_fetch_blocks_after_q(
        peer_manager, blockchain_view, filter_f=None):

    inv_batcher = InvBatcher(peer_manager)

    # return a Queue that gets filled with blocks until we run out

    peer_to_header_q = create_peer_to_header_q(peer_manager, blockchain_view, inv_batcher)

    header_to_block_future_q = create_header_to_block_future_q(
        inv_batcher, input_q=peer_to_header_q, filter_f=filter_f)

    async def header_to_block(next_item, q):
        if next_item is None:
            await q.put(None)
            peer_to_header_q.stop()
            header_to_block_future_q.stop()
            await peer_to_header_q.wait()
            await header_to_block_future_q.drain()
            return
        block_future, index = next_item
        block = await block_future
        if hasattr(block, "tx_futures"):
            txs = []
            for f in block.tx_futures:
                txs.append(await f)
            block.txs = txs
        await q.put((block, index))

    block_index_q = MappingQueue(
        dict(callback_f=header_to_block, input_q=header_to_block_future_q, worker_count=1)
    )

    return block_index_q


def fetch_blocks_after(peer_manager, blockchain_view, filter_f=None):

    # yields blocks until we run out
    loop = asyncio.get_event_loop()

    block_index_q = create_fetch_blocks_after_q(peer_manager, blockchain_view, filter_f)

    while True:
        v = loop.run_until_complete(block_index_q.get())
        if v is None:
            break
        yield v
