import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.MappingQueue import MappingQueue



def make_event_q(peer, block_hash_to_future):

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
                if block_hash in block_hash_to_future:
                    f = block_hash_to_future[block_hash]
                    if not f.done():
                        f.set_result(block)

    asyncio.get_event_loop().create_task(event_loop(peer))
    return headers_msg_q


async def block_catchup(peers, bcv, hash_stop=b'\0'*32):

    loop = asyncio.get_event_loop()

    block_hash_to_future = dict()

    peer_to_block_pipe = asyncio.Queue()

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
            f = loop.create_future()
            item = (0, headers[0].previous_block_hash, f, set())
            await block_future_queue.put(item)
            extra_block = await f
            headers = [extra_block] + headers

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
        await peer_to_block_pipe.put(pair)

    async def create_block_future(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_hashes = item
        logging.info("got %d new header(s) starting at %d" % (len(block_hashes), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_hashes)]

        for bh, pri in block_hash_priority_pair_list:
            if pri < 200000:
                continue
            f = block_hash_to_future.get(bh) or asyncio.Future()
            peers_tried = set()
            item = (pri, bh, f, peers_tried)
            await block_future_queue.put(item)
            await q.put(f)

    async def wait_future(future, q):
        if future is None:
            await q.put(None)
            return
        block = await future
        del block_hash_to_future[block.hash()]
        await q.put(block)

    peer_to_block_pipe = MappingQueue(
        dict(callback_f=improve_headers, worker_count=1, input_q=peer_to_block_pipe),
        dict(callback_f=create_block_future, worker_count=1, input_q_maxsize=2),
        dict(callback_f=wait_future, worker_count=1, input_q_maxsize=1000),
    )

    async def batch_block_fetches(peer_tuple, q):
        peer, max_batch_size = peer_tuple
        batch = []
        skipped = []
        while len(batch) == 0 or (len(batch) < max_batch_size and not block_future_queue.empty()):
            item = await block_future_queue.get()
            (pri, bh, f, peers_tried) = item
            if f.done():
                continue
            if peer in peers_tried:
                skipped.append(item)
            else:
                batch.append(item)
            block_hash_to_future[bh] = f
        if len(batch) > 0:
            await q.put((peer, batch, max_batch_size))
        for item in skipped:
            if not item[2].done:
                await block_future_queue.put(item)

    TARGET_BATCH_TIME = 10
    MIN_BATCH_SIZE = 1
    MAX_BATCH_SIZE = 500
    batch_timeout = 15

    async def fetch_batch(peer_batch, q):
        peer, batch, max_batch_size = peer_batch
        inv_items = [InvItem(ITEM_TYPE_BLOCK, bh) for (pri, bh, f, peers_tried) in batch]
        peer.send_msg("getdata", items=inv_items)
        start_time = loop.time()
        futures = [f for (pri, bh, f, peers_tried) in batch]
        await asyncio.wait(futures, timeout=batch_timeout)
        end_time = loop.time()
        batch_time = end_time - start_time
        logging.info("completed batch size of %d with time %f", len(inv_items), batch_time)
        time_per_item = batch_time / len(inv_items)
        new_batch_size = min(max(MIN_BATCH_SIZE, int(TARGET_BATCH_TIME / time_per_item + 0.5)), MAX_BATCH_SIZE)
        logging.info("new batch size for %s is %d", peer, new_batch_size)
        max_batch_size = new_batch_size
        for item in batch:
            if not item[2].done():
                item[3].add(peer)
                await block_future_queue.put(item)
        await peer_batch_queue.put((peer, max_batch_size))

    block_future_queue = asyncio.PriorityQueue(maxsize=1000)

    peer_batch_queue = MappingQueue(
        dict(callback_f=batch_block_fetches),
        dict(callback_f=fetch_batch, input_q_maxsize=2),
    )

    async def get_next_event():
        name, data = await peer.next_message(unpack_to_dict=True)
        if name == 'ping':
            peer.send_msg("pong", nonce=data["nonce"])
        if name == 'headers':
            await headers_msg_q.put((name, data))
        if name in ("block", "merkleblock"):
            block = data["block"]
            block_hash = block.hash()
            if block_hash in block_hash_to_future:
                f = block_hash_to_future[block_hash]
                if not f.done():
                    f.set_result(block)

    for peer in peers:
        headers_msg_q = make_event_q(peer, block_hash_to_future)
        await peer_to_block_pipe.put((peer, headers_msg_q))
        await peer_batch_queue.put((peer, MIN_BATCH_SIZE))
        await peer_batch_queue.put((peer, MIN_BATCH_SIZE))

    idx = 0
    while True:
        block = await peer_to_block_pipe.get()
        if block is None:
            break
        print("%d : %s" % (idx, block))
        idx += 1


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
        await block_catchup(peers, bcv)

    asyncio.get_event_loop().run_until_complete(go())


if __name__ == '__main__':
    main()
