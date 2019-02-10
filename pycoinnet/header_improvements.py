import logging

from aiter import iter_to_aiter, push_aiter, join_aiters, gated_aiter, stoppable_aiter


async def monitor_improvements(peer_manager, blockchain_view, block_batcher):
    async for peer, message, data in peer_manager.new_event_aiter():
        if message != "headers":
            continue

        headers = [bh for bh, t in data["headers"]]

        if len(headers) == 0:
            yield peer, None, []
            continue

        while (headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1] and
                blockchain_view.tuple_for_hash(headers[0].hash()) is None):
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await block_batcher._add_to_download_queue(bh, 0)
            peer, block = await f
            headers = [block] + headers

        block_number = blockchain_view.do_headers_improve_path(headers)
        if block_number is False:
            continue

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(blockchain_view.last_block_index()+1-block_number):
            the_tuple = blockchain_view.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])

        logging.info("got %d new header(s) starting at %d" % (len(hashes), block_number))
        yield peer, block_number, hashes


async def header_improvements_aiter(peer_manager, blockchain_view, block_batcher, catch_up_count):
    """
    yields triples of (peer, block_index, hashes)

    stops when peer manager runs out of peers
    """
    caught_up_peers = set()

    def request_headers_from_peer(peer, blockchain_view):
        if peer in caught_up_peers:
            return
        block_locator_hashes = blockchain_view.block_locator_hashes()
        hash_stop = blockchain_view.hash_initial_block()
        logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])
        peer.send_msg("getheaders", version=1, hashes=block_locator_hashes, hash_stop=hash_stop)

    async def peer_aiter_to_triple(aiter):
        async for _ in aiter:
            yield _, None, None

    async def make_alt_peer_aiter(peer_manager, peer):
        while True:
            # make sure there exist SOME peer besides the given one
            async for _ in peer_manager.new_peer_aiter():
                if _ != peer:
                    break

            async for _ in peer_manager.active_peers_aiter():
                if _ != peer:
                    yield _

    monitor_improvements_aiter = monitor_improvements(peer_manager, blockchain_view, block_batcher)

    aiter_of_aiters = push_aiter()
    joined_aiter = join_aiters(aiter_of_aiters)

    alt_peer_aiter = stoppable_aiter(peer_manager.new_peer_aiter())
    aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))

    aiter_of_aiters.push(monitor_improvements_aiter)

    best_peer = None

    async for peer, block_index, hashes in joined_aiter:
        if hashes is None:
            request_headers_from_peer(peer, blockchain_view)
            continue

        yield (peer, block_index, hashes)

        if len(hashes) == 0:
            logging.debug("caught up headers on peer %s", peer)
            caught_up_peers.add(peer)
            if len(caught_up_peers) >= catch_up_count:
                break
            if peer == best_peer:
                best_peer = None
                alt_peer_aiter = stoppable_aiter(peer_manager.new_peer_aiter())
                aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))
            continue

        alt_peer_aiter.stop()
        alt_peer_aiter = stoppable_aiter(peer_manager.new_peer_aiter())
        aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))
        continue

        # case 1: we got a message from something other than our best_peer
        if best_peer != peer:
            logging.debug("got a new best headers peer %s (was %s)", peer, best_peer)
            # reset the alt list
            alt_peer_aiter.stop()

            best_peer = peer

            # retry this peer
            aiter_of_aiters.push(peer_aiter_to_triple(iter_to_aiter([best_peer])))

            # set up the alt peer aiter
            alt_peer_aiter = gated_aiter(make_alt_peer_aiter(peer_manager, peer))
            aiter_of_aiters.push(peer_aiter_to_triple(alt_peer_aiter))
            alt_peer_aiter.push(1)
            continue

        # case 2: we got a message from our best_peer
        else:
            # 2a: we got an improvement
            # query it again
            aiter_of_aiters.push(peer_aiter_to_triple(iter_to_aiter([best_peer])))
            # and get another alt peer
            alt_peer_aiter.push(1)
            continue

    aiter_of_aiters.stop()
