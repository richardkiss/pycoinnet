import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK
from pycoinnet.MappingQueue import MappingQueue


def _make_event_q(peer, inv_item_hash_to_future):

    headers_msg_q = asyncio.Queue()

    async def event_loop(peer):
        while True:
            name, data = await peer.next_message(unpack_to_dict=True)
            if name == 'ping':
                peer.send_msg("pong", nonce=data["nonce"])
            if name == 'headers':
                await headers_msg_q.put((name, data))
            if name in ("block", "merkleblock"):
                block = data["block"]
                block_hash = block.hash()
                inv_item = InvItem(ITEM_TYPE_BLOCK if name == "block" else ITEM_TYPE_MERKLEBLOCK, block_hash)
                if inv_item in inv_item_hash_to_future:
                    f = inv_item_hash_to_future[inv_item]
                    if not f.done():
                        f.set_result(block)
                else:
                    logging.error("missing future for block %s", block.id())

    headers_msg_q.task = asyncio.get_event_loop().create_task(event_loop(peer))
    return headers_msg_q


def _create_peer_batch_queue(target_batch_time=10, max_batch_size=500):

    inv_item_future_queue = asyncio.PriorityQueue(maxsize=1000)

    async def batch_block_fetches(peer_batch_tuple, q):
        peer, desired_batch_size = peer_batch_tuple
        batch = []
        skipped = []
        logging.info("peer %s trying to build batch up to size %d", peer, desired_batch_size)
        while len(batch) == 0 or (len(batch) < desired_batch_size and not inv_item_future_queue.empty()):
            item = await inv_item_future_queue.get()
            (priority, inv_item, f, peers_tried) = item
            if f.done():
                continue
            if peer in peers_tried:
                skipped.append(item)
            else:
                batch.append(item)
        if len(batch) > 0:
            await q.put((peer, batch, desired_batch_size))
        for item in skipped:
            if not item[2].done:
                await inv_item_future_queue.put(item)

    async def fetch_batch(peer_batch, q):
        loop = asyncio.get_event_loop()
        peer, batch, prior_max = peer_batch
        inv_items = [inv_item for (priority, inv_item, f, peers_tried) in batch]
        peer.send_msg("getdata", items=inv_items)
        start_time = loop.time()
        futures = [f for (priority, bh, f, peers_tried) in batch]
        await asyncio.wait(futures, timeout=target_batch_time)
        end_time = loop.time()
        batch_time = end_time - start_time
        logging.info("completed batch size of %d with time %f", len(inv_items), batch_time)
        completed_count = sum([1 for f in futures if f.done()])
        item_per_unit_time = completed_count / batch_time
        new_batch_size = min(prior_max * 4, int(target_batch_time * item_per_unit_time + 0.5))
        new_batch_size = min(max(1, new_batch_size), max_batch_size)
        logging.info("new batch size for %s is %d", peer, new_batch_size)
        for (priority, inv_item, f, peers_tried) in batch:
            if not f.done():
                peers_tried.add(peer)
                await inv_item_future_queue.put((priority, inv_item, f, peers_tried))
        await peer_batch_queue.put((peer, new_batch_size))

    peer_batch_queue = MappingQueue(
        dict(callback_f=batch_block_fetches),
        dict(callback_f=fetch_batch, input_q_maxsize=2),
    )

    inv_item_hash_to_future = dict()

    async def inv_future_for_hash(inv_item):
        f = inv_item_hash_to_future.get(inv_item)
        if not f:
            f = asyncio.Future()
            inv_item_hash_to_future[inv_item] = f

            def remove_later(f):

                def remove():
                    if inv_item in inv_item_hash_to_future:
                        del inv_item_hash_to_future[inv_item]

                asyncio.get_event_loop().call_later(5, remove)

            f.add_done_callback(remove_later)
        item = (0, inv_item, f, set())
        await inv_item_future_queue.put(item)
        return f

    return peer_batch_queue, inv_future_for_hash, inv_item_hash_to_future


def create_peer_to_block_pipe(bcv, hash_stop):
    """
    return a MappingQueue that accepts: peer objects
    and yields: block objects
    """

    improve_headers_pipe = asyncio.Queue()

    peer_batch_queue, inv_future_for_hash, inv_item_hash_to_future = _create_peer_batch_queue()

    async def note_peer(peer, q):
        initial_batch_size = 1
        headers_msg_q = _make_event_q(peer, inv_item_hash_to_future)
        pair = (peer, headers_msg_q)
        await peer_batch_queue.put((peer, initial_batch_size))
        await peer_batch_queue.put((peer, initial_batch_size))
        await q.put(pair)

    async def improve_headers(pair, q):
        peer, headers_msg_q = pair
        block_locator_hashes = bcv.block_locator_hashes()
        logging.debug("getting headers after %d", bcv.last_block_tuple()[0])
        peer.send_msg(
            message_name="getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = await headers_msg_q.get()
        headers = [bh for bh, t in data["headers"]]

        if block_locator_hashes[-1] == bcv.hash_initial_block():
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            f = await inv_future_for_hash(InvItem(ITEM_TYPE_BLOCK, headers[0].previous_block_hash))
            block = await f
            headers = [block] + headers

        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            await q.put(None)
            return

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(block_number, bcv.last_block_index()+1):
            the_tuple = bcv.tuple_for_index(idx)
            assert the_tuple[0] == idx
            hashes.append(the_tuple[1])
        await q.put((block_number, hashes))
        await improve_headers_pipe.put(pair)

    async def create_block_future(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_hashes = item
        logging.info("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]

        # TODO: put an extra layer here to let the caller decide what to do
        # either download the full block, the merkle block, or skip it
        for bh, pri in block_hash_priority_pair_list:
            if pri < 200000:
                continue
            f = await inv_future_for_hash(InvItem(ITEM_TYPE_BLOCK, bh))
            await q.put(f)

    async def wait_future(future, q):
        if future is None:
            await q.put(None)
            return
        block = await future
        await q.put(block)

    peer_to_block_pipe = MappingQueue(
        dict(callback_f=note_peer),
        dict(callback_f=improve_headers, worker_count=1, input_q=improve_headers_pipe),
        dict(callback_f=create_block_future, worker_count=1, input_q_maxsize=2),
        dict(callback_f=wait_future, worker_count=1, input_q_maxsize=1000),
    )
    return peer_to_block_pipe


def main():
    from pycoinnet.cmds.common import peer_connect_pipeline, init_logging
    from pycoinnet.BlockChainView import BlockChainView
    from pycoinnet.networks import MAINNET

    async def go():
        init_logging()
        bcv = BlockChainView()

        peers = []

        if 1:
            peer_q = peer_connect_pipeline(MAINNET, tcp_connect_workers=20)
            peers.append(await peer_q.get())
            #peer_q.cancel()
            import pdb; pdb.set_trace()

        if 1:
            host_q = asyncio.Queue()
            host_q.put_nowait(("192.168.1.99", 8333))
            peer_q = peer_connect_pipeline(MAINNET, host_q=host_q)
            peers.append(await peer_q.get())

        hash_stop = b'\0'*32
        peer_to_block_pipe = create_peer_to_block_pipe(bcv, hash_stop)

        for peer in peers:
            await peer_to_block_pipe.put(peer)

        idx = 0
        while True:
            block = await peer_to_block_pipe.get()
            if block is None:
                break
            print("%d : %s" % (idx, block))
            idx += 1

    asyncio.get_event_loop().run_until_complete(go())


if __name__ == '__main__':
    main()
