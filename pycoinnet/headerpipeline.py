import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.BlockChainView import HASH_INITIAL_BLOCK


@asyncio.coroutine
def _fetch_missing(peer, header):
    the_hash = header.previous_block_hash
    inv_item = InvItem(ITEM_TYPE_BLOCK, the_hash)
    logging.info("requesting missing block header %s", inv_item)
    peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, the_hash)])
    name, data = yield from peer.wait_for_response('block')
    block = data["block"]
    logging.info("got missing block %s", block.id())
    return block


@asyncio.coroutine
def improve_headers(peer, bcv, update_q, hash_stop=b'\0'*32):
    """
    Ask the given peer if the headers can be improved. If so, add a pair
    (new_block_number, hashes) to the update_q.

    peer: the peer to send the getheaders message to
    bcv: BlockChainView instance, the view of the block chain to try to improve upon
    update_q: asyncio.Queue type, where the pair is pushed
    hash_stop: sent in the getheaders message, not sure why you'd change this

    Returns once subsequent calls to getheaders don't improve anything.
    """
    while True:
        block_locator_hashes = bcv.block_locator_hashes()
        logging.debug("getting headers after %d", bcv.last_block_tuple()[0])
        peer.send_msg(
            message_name="getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = yield from peer.wait_for_response('headers')
        headers = [bh for bh, t in data["headers"]]

        if len(headers) == 0:
            return

        if block_locator_hashes[-1] == HASH_INITIAL_BLOCK:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            extra_block = yield from _fetch_missing(peer, headers[0])
            headers = [extra_block] + headers

        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            continue
        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(block_number, bcv.last_block_index()+1):
            the_tuple = bcv.tuple_for_index(idx)
            assert the_tuple[0] == idx
            hashes.append(the_tuple[1])
        update_q.put_nowait((block_number, hashes))

