"""
This code takes care of calling "getheaders" repeatedly to quickly
catch up the local copy of the block chain. It does not get full blocks.
"""

import asyncio
import logging
import time

from pycoinnet.InvItem import InvItem, ITEM_TYPE_BLOCK
from pycoinnet.helpers.standards import get_headers_hashes


def fast_forwarder_add_peer_f(blockchain):
    """
    returns a function "add_peer"
        add_peer(peer, last_block_index)
    which accepts a BitcoinPeerProtocol peer and the last_block_index
    as reported in the initial handshake.
    """
    peer_queue = asyncio.PriorityQueue()

    @asyncio.coroutine
    def _fetch_missing(peer, blockchain):
        next_message = peer.new_get_next_message_f(lambda msg, data: msg == 'block')
        ops = []
        for h in blockchain.chain_finder.missing_parents():
            peer.send_msg("getdata", items=[InvItem(ITEM_TYPE_BLOCK, h)])
            msg, data = yield from next_message()
            block = data["block"]
            ops = blockchain.add_headers([block])
            if len(ops) > 0:
                break
        return ops

    @asyncio.coroutine
    def _run_ff(blockchain):
        # this kind of works, but it's lame because we put
        # peers into a queue, so they'll never be garbage collected
        # even if they vanish. I think.
        while 1:
            priority, (peer, lbi, rate_dict) = yield from peer_queue.get()
            if lbi - blockchain.length() > 0:
                # let's get some headers from this guy!
                start_time = time.time()
                h = blockchain.last_block_hash()
                try:
                    headers = yield from asyncio.wait_for(get_headers_hashes(peer, h), timeout=10)
                except EOFError:
                    # this peer is dead... so don't put it back in the queue
                    continue
                except asyncio.TimeoutError:
                    # this peer timed out. How useless
                    continue
                # TODO: what if the stupid client sends us bogus headers?
                # how will we ever figure this out?
                # answer: go through headers and remove fake ones or ones that we've seen
                # check hash, difficulty, difficulty against hash, and that they form
                # a chain. This make it expensive to produce bogus headers
                time_elapsed = time.time() - start_time
                rate_dict["total_seconds"] += time_elapsed
                rate_dict["records"] += len(headers)
                priority = - rate_dict["records"] / rate_dict["total_seconds"]
                # let's make sure we actually extend the chain
                ops = blockchain.add_headers(headers)
                ## this hack is necessary because the stupid default client
                # does not send the genesis block!
                try:
                    while len(ops) == 0:
                        ops = yield from asyncio.wait_for(_fetch_missing(peer, blockchain), timeout=30)
                except Exception:
                    logging.exception("problem fetching missing parents")
                if len(ops) > 0:
                    peer_queue.put_nowait((priority, (peer, lbi, rate_dict)))
                # otherwise, this peer is stupid and should be ignored

    def add_peer(peer, last_block_index):
        peer_queue.put_nowait((0, (peer, last_block_index, dict(total_seconds=0, records=0))))

    # we need a strong reference to this task
    add_peer.task = asyncio.Task(_run_ff(blockchain))

    return add_peer
