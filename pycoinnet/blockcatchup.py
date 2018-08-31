import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.aitertools import q_aiter, aiter_to_iter, flatten_aiter, map_aiter, join_aiters
from pycoinnet.inv_batcher import InvBatcher


class RequestResponder:
    def __init__(self, peer_manager):
        self._callback_handle = peer_manager.add_event_callback(self.event_callback)
        self._response_futures = dict()

    def event_callback(self, peer, message, data):
        if (peer, message) in self._response_futures:
            if not self._response_futures[(peer, message)].done():
                self._response_futures[(peer, message)].set_result((peer, data))

    def request_response(self, peer, request_message, response_message, **kwargs):
        self._response_futures[(peer, response_message)] = asyncio.Future()
        peer.send_msg(request_message, **kwargs)
        return self._response_futures[(peer, response_message)]


async def get_peers_to_query(best_peer, caught_up_peers, desired_caught_up_peer_count, peer_q, peer_manager):

    peers_to_query, peers_needed = [], desired_caught_up_peer_count
    if best_peer:
        peers_to_query, peers_needed = [best_peer], 2

    while len(peers_to_query) < peers_needed:
        if peer_q.empty():
            for p in peer_manager.peers():
                if p and p not in peers_to_query and p not in caught_up_peers and not p.is_closing():
                    peer_q.put_nowait(p)
        if peer_q.empty():
            # still empty
            break
        peer = await peer_q.get()
        if peer not in peers_to_query and peer is not None:
            peers_to_query.append(peer)
    return peers_to_query


async def create_header_fetcher(peer_manager, header_q, inv_batcher, blockchain_view, timeout, desired_caught_up_peer_count=8):
    """
    Given a peer_manager (which is a set of peers) and a blockchain view, we query peers
    for headers messages until enough of them say we're all caught up.
    """

    # this RequestResponder is a gross hack, here for now. We need a better home for this functionaliy. Maybe PeerManager.

    rr = RequestResponder(peer_manager)

    # the list of peers that think we're all caught up
    caught_up_peers = set()

    peer_q = asyncio.Queue()

    def fill_peer_q(peer, message, data):
        if message is None:
            peer_q.put_nowait(peer)

    fpq_handle = peer_manager.add_event_callback(fill_peer_q)

    best_peer = None

    while True:
        peers_to_query = await get_peers_to_query(
            best_peer, caught_up_peers, desired_caught_up_peer_count, peer_q, peer_manager)

        if len(peers_to_query) == 0:
            if len(caught_up_peers) >= desired_caught_up_peer_count:
                break
            best_peer = await peer_q.get()
            continue

        # we have peers_to_query set

        # try to improve things

        headers = []
        block_locator_hashes = blockchain_view.block_locator_hashes()
        hash_stop = blockchain_view.hash_initial_block()
        logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])

        tasks = []
        for peer in peers_to_query:
            task = rr.request_response(
                peer, "getheaders", "headers", version=1,
                hashes=block_locator_hashes, hash_stop=hash_stop)
            tasks.append(task)
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        if done:
            best_peer, data = await done.pop()
            headers = [bh for bh, t in data["headers"]]
        for t in pending:
            t.cancel()

        while (len(headers) > 0 and
                headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1]):
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
            block = await f
            headers = [block] + headers

        block_number = blockchain_view.do_headers_improve_path(headers)
        if block_number is False:
            # this peer has exhausted its view
            caught_up_peers.add(peer)
            best_peer = None
            continue

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(blockchain_view.last_block_index()+1-block_number):
            the_tuple = blockchain_view.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])

        await header_q.put((block_number, hashes))

    header_q.stop()


def make_headers_info_aiter(peer_manager, inv_batcher, blockchain_view, timeout, desired_caught_up_peer_count):
    header_q = q_aiter(maxsize=2)
    header_q.task = asyncio.ensure_future(create_header_fetcher(
        peer_manager, header_q, inv_batcher, blockchain_view, timeout, desired_caught_up_peer_count))
    return header_q


def make_map_bibh_to_fi(inv_batcher, filter_f):
    async def map_bibh_to_fi(item):
        """
        Map (block_index, block_headers) pairs to a list of (f, block_index) future/int pairs.
        """
        first_block_index, block_headers = item
        logging.info("got %d new header(s) starting at %d" % (len(block_headers), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_headers)]

        results = []
        for bh, block_index in block_hash_priority_pair_list:
            item_type = filter_f(bh, block_index)
            if item_type:
                f = await inv_batcher.inv_item_to_future(
                    InvItem(item_type, bh.hash()), priority=block_index)
            else:
                f = asyncio.Future()
                f.set_result(bh)
            results.append((f, block_index))
        return results
    return map_bibh_to_fi


async def future_to_block(next_item):
    block_future, index = next_item
    block = await block_future
    if hasattr(block, "tx_futures"):
        txs = []
        for f in block.tx_futures:
            txs.append(await f)
        block.txs = txs
    return (block, index)


def create_fetch_blocks_after_aiter(
        peer_manager, blockchain_view, peer_count, filter_f=None, timeout=10.0, loop=None):
    # return a Queue that gets filled with blocks until we run out

    inv_batcher = InvBatcher(peer_manager)

    filter_f = filter_f or (lambda block_hash, index: ITEM_TYPE_BLOCK)

    map_bibh_to_fi = make_map_bibh_to_fi(inv_batcher, filter_f)

    block_index_aiter = map_aiter(future_to_block, join_aiters(flatten_aiter(
        map_aiter(map_bibh_to_fi, join_aiters(
            make_headers_info_aiter(
                peer_manager, inv_batcher, blockchain_view, timeout, peer_count), maxsize=2),
                    )), maxsize=100))

    return block_index_aiter


def fetch_blocks_after(peer_manager, blockchain_view, peer_count, filter_f=None):
    return aiter_to_iter(create_fetch_blocks_after_aiter(peer_manager, blockchain_view, peer_count, filter_f))
