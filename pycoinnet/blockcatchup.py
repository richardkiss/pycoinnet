import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.MappingQueue import MappingQueue

from pycoinnet.inv_batcher import InvBatcher


def _make_event_q(peer, inv_batcher):

    headers_msg_q = asyncio.Queue()

    async def event_loop(peer):
        while True:
            name, data = await peer.next_message(unpack_to_dict=True)
            inv_batcher.handle_event(peer, name, data)
            if name == 'ping':
                peer.send_msg("pong", nonce=data["nonce"])
            if name == 'headers':
                await headers_msg_q.put((name, data))

    headers_msg_q.task = asyncio.get_event_loop().create_task(event_loop(peer))
    return headers_msg_q


def create_peer_to_block_pipe(bcv, hash_stop):
    """
    return a MappingQueue that accepts: peer objects
    and yields: block objects
    """

    inv_batcher = InvBatcher()

    improve_headers_pipe = asyncio.Queue()

    async def note_peer(peer, q):
        await inv_batcher.add_peer(peer)
        headers_msg_q = _make_event_q(peer, inv_batcher)
        pair = (peer, headers_msg_q)
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
            f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
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
