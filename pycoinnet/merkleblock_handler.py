import logging

from aiter import aiter_to_iter
from pycoin.message.InvItem import InvItem, ITEM_TYPE_MERKLEBLOCK

from pycoinnet.header_improvements import header_improvements_aiter

from .BlockBatcher import BlockBatcher


# API:
#  - request a merkle block from a specific peer with a given filter context
#  - return a future
#  - future turns into merkle block with transactions and the filter context
#  - that way, the context can be analyzed and checked to see that it's deep enough
#  - how do we query the context?
#    - we need a way to name the context
#    - a pair of "last addresses", deposit + change
#    - so, two indices indicating the last included
#    - then we just compare to the last two needed
#    - we have a "gap limit" (minimum unused addresses) and "padding" (extra unused address beyond gap)
#    - maybe an "address creator" type, which is an address creator + an index
#    - so a standard wallet is a pair of address creators, one for deposit, one for change


async def merkleblocks_for_event_aiter(peer, event_aiter):
    """
    Turn an event aiter, like that from a peer_manager, into a (peer, merkleblock) iterator.
    """

    tx_ids_pending = set()
    pending_merkleblock = None

    async for _peer, message, data in event_aiter:

        if _peer != peer:
            continue

        if message == "merkleblock":
            if pending_merkleblock:
                logging.error("merkleblock %s discarded", pending_merkleblock)
            pending_merkleblock = data["header"]
            pending_merkleblock.txs = []
            tx_ids_pending = set(data["tx_hashes"])

        elif message == "tx":
            tx = data["tx"]
            h = tx.hash()
            if h in tx_ids_pending:
                tx_ids_pending.discard(h)
                pending_merkleblock.txs.append(tx)

        if pending_merkleblock and len(tx_ids_pending) == 0:
            yield pending_merkleblock
            pending_merkleblock = None


# what we want is a blockhash aiter => merkleblock aiter filter (for ONE peer)
# what we have so far is a peer_manger => merkleblock aiter (for ALL peers)
# we can apply a tee_aiter and then a filter on the aiter to go
#   peer_manager => merkleblock aiter for ONE peer


async def merkleblockcatchup(peer_manager, blockchain_view, count, skip_date):
    # we will use just one peer for now
    peer = None
    async for peer in peer_manager.new_peer_aiter():
        break
    if peer is None:
        breakpoint()

    block_batcher = BlockBatcher(peer_manager)

    header_aiter = header_improvements_aiter(peer_manager, blockchain_view, block_batcher, count)

    merkleblock_aiter = merkleblocks_for_event_aiter(peer, peer_manager.new_event_aiter()).__aiter__()

    async for peer, start_index, headers in header_aiter:
        for idx, h in enumerate(headers):
            inv_items = [InvItem(ITEM_TYPE_MERKLEBLOCK, h.hash())]
            peer.send_msg("getdata", items=inv_items)
            try:
                mb = await merkleblock_aiter.__anext__()
                yield peer, mb, start_index + idx
            except StopAsyncIteration:
                break


def fetch_merkleblocks_after(peer_manager, blockchain_view, peer_count, skip_date):
    for peer, block, index in aiter_to_iter(merkleblockcatchup(
            peer_manager, blockchain_view, peer_count, skip_date)):
        yield block, index
