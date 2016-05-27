"""
This code takes care of calling "getheaders" repeatedly to quickly
catch up the local copy of the block chain. It does not get full blocks.
"""

import asyncio
import logging
import time

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.helpers.standards import do_get_headers
from pycoinnet.util.BlockChainView import HASH_INITIAL_BLOCK


def getheaders_add_peer_f(blockchain_view, improved_blockchain_view_callback_f):
    """
    returns a function "add_peer"
        add_peer(peer, last_block_index)
    which accepts a BitcoinPeerProtocol peer and the last_block_index
    as reported in the initial handshake.
    """
    peer_queue = asyncio.PriorityQueue()

    @asyncio.coroutine
    def _fetch_missing(peer, header):
        next_message = peer.new_get_next_message_f(lambda msg, data: msg == 'block')
        the_hash = header.previous_block_hash
        peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, the_hash)])
        msg, data = yield from next_message()
        block = data["block"]
        return block

    @asyncio.coroutine
    def _run_ff(blockchain_view):
        # this kind of works, but it's lame because we put
        # peers into a queue, so they'll never be garbage collected
        # even if they vanish. I think.
        reorg_headers = []
        while 1:
            priority, (peer, lbi, rate_dict) = yield from peer_queue.get()
            if lbi - blockchain_view.last_block_index() > 0:
                # let's get some headers from this guy!
                start_time = time.time()
                block_locator_hashes = blockchain_view.block_locator_hashes()
                logging.debug("block_locator_hashes: %s", block_locator_hashes)
                try:
                    headers = yield from asyncio.wait_for(
                        do_get_headers(peer, block_locator_hashes), timeout=10)
                except EOFError:
                    # this peer is dead... so don't put it back in the queue
                    continue
                except asyncio.TimeoutError:
                    # this peer timed out. How useless
                    continue

                if block_locator_hashes[-1] == HASH_INITIAL_BLOCK:
                    # this hack is necessary because the stupid default client
                    # does not send the genesis block!
                    extra_block = yield from asyncio.wait_for(_fetch_missing(peer, headers[0]), timeout=30)
                    headers = [extra_block] + headers

                # TODO: validate difficulties
                # this will actually be difficult since it depends upon the prior 2016 block headers

                time_elapsed = time.time() - start_time
                rate_dict["total_seconds"] += time_elapsed
                rate_dict["records"] += len(headers)
                priority = - rate_dict["records"] / rate_dict["total_seconds"]

                reorg_headers.extend(headers)
                block_number = blockchain_view.do_headers_improve_path(reorg_headers)
                if block_number is None:
                    if len(headers) == 0:
                        # this peer is stupid and should be ignored
                        logging.info("got to end of header chain for %s but no reorg was needed", peer)
                        reorg_headers = []
                    else:
                        # continue using this peer
                        peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))
                else:
                    logging.debug(
                        "improved_blockchain_view_callback_f with block number %d and %d headers",
                        block_number, len(reorg_headers))
                    blockchain_view.winnow()
                    yield from improved_blockchain_view_callback_f(block_number, reorg_headers)
                    reorg_headers = []
                    peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))

    def add_peer(peer, last_block_index):
        peer_queue.put_nowait((0, (peer, last_block_index, dict(total_seconds=0, records=0))))

    # we need a strong reference to this task
    add_peer.task = asyncio.Task(_run_ff(blockchain_view))

    return add_peer
