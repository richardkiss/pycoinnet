import asyncio
import logging

from pycoin.message.InvItem import InvItem, ITEM_TYPE_BLOCK

from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.dnsbootstrap import dns_bootstrap_host_port_q
from pycoinnet.inv_batcher import InvBatcher
from pycoinnet.MappingQueue import MappingQueue
from pycoinnet.Peer import Peer
from pycoinnet.pong_manager import install_pong_manager
from pycoinnet.version import version_data_for_peer


def parser_and_packer_for_network(network):
    # BRAIN DAMAGE
    from pycoin.message.make_parser_and_packer import (
        make_parser_and_packer, standard_messages,
        standard_message_post_unpacks, standard_streamer, standard_parsing_functions
    )

    streamer = standard_streamer(standard_parsing_functions(network.block, network.tx))
    parser, packer = make_parser_and_packer(
        streamer, standard_messages(), standard_message_post_unpacks(streamer))

    def new_parser(message, data):
        try:
            return parser(message, data)
        except KeyError:
            logging.error("unknown message %s", message)
            return b''

    return new_parser, packer


def create_peer_to_header_q(index_hash_work_tuples, inv_batcher, loop=None):
    # create and return a Mapping Queue that takes peers as input
    # and produces tuples of (initial_block, [headers]) as output

    blockchain_view = BlockChainView(index_hash_work_tuples)

    peer_q = asyncio.Queue()

    async def peer_to_header_tuples(peer, q):
        block_locator_hashes = blockchain_view.block_locator_hashes()
        hash_stop = blockchain_view.hash_initial_block()
        logging.debug("getting headers after %d", blockchain_view.last_block_tuple()[0])
        # BRAIN DAMAGE: need a timeout here
        data = await peer.request_response(
            "getheaders", "headers", version=1,
            hashes=block_locator_hashes, hash_stop=hash_stop)
        headers = [bh for bh, t in data["headers"]]

        while len(headers) > 0 and headers[0].previous_block_hash != blockchain_view.last_block_tuple()[1]:
            # this hack is necessary because the stupid default client
            # does not send the genesis block!
            bh = headers[0].previous_block_hash
            f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
            block = await f
            headers = [block] + headers

        block_number = blockchain_view.do_headers_improve_path(headers)
        if block_number is False:
            if peer_q.qsize() == 0:
                await q.put(None)
            # this peer has exhausted its view
            return

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
            if not item_type:
                continue
            f = await inv_batcher.inv_item_to_future(InvItem(item_type, bh.hash()), priority=block_index)
            await q.put((f, block_index))

    return MappingQueue(
        dict(callback_f=create_block_hash_entry, input_q=input_q, worker_count=1),
        final_q=asyncio.Queue(maxsize=500), loop=loop)


def peer_connect_pipeline(network, tcp_connect_workers=30, handshake_workers=3,
                          host_q=None, loop=None, version_dict={}):

    host_q = host_q or dns_bootstrap_host_port_q(network)

    async def do_tcp_connect(host_port_pair, q):
        host, port = host_port_pair
        logging.debug("TCP connecting to %s:%d", host, port)
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            logging.debug("TCP connected to %s:%d", host, port)
            await q.put((reader, writer))
        except Exception as ex:
            logging.info("connect failed: %s:%d (%s)", host, port, ex)

    async def do_peer_handshake(rw_tuple, q):
        reader, writer = rw_tuple
        parse_from_data, pack_from_data = parser_and_packer_for_network(network)
        peer = Peer(
            reader, writer, network.magic_header, parse_from_data,
            pack_from_data, max_msg_size=10*1024*1024)
        version_data = version_data_for_peer(peer, **version_dict)
        peer.version = await peer.perform_handshake(**version_data)
        if peer.version is None:
            logging.info("handshake failed on %s", peer)
            peer.close()
        else:
            await q.put(peer)

    filters = [
        dict(callback_f=do_tcp_connect, input_q=host_q, worker_count=tcp_connect_workers),
        dict(callback_f=do_peer_handshake, worker_count=handshake_workers),
    ]
    return MappingQueue(*filters, loop=loop)


def get_peer_pipeline(network, peer_addresses, peer_q):
    # for now, let's just do one peer
    host_q = None
    if peer_addresses:
        host_q = asyncio.Queue()
        for peer in peer_addresses:
            if ":" in peer:
                host, port = peer.split(":", 1)
                port = int(port)
            else:
                host = peer
                port = network.default_port
            host_q.put_nowait((host, port))
    # BRAIN DAMAGE: 70016 version number is required for bgold new block header format
    return peer_connect_pipeline(network, host_q=host_q, version_dict=dict(version=70015))


def fetch_blocks_after(
        network, index_hash_work_tuples, peer_addresses=None,
        filter_f=lambda bh, index: ITEM_TYPE_BLOCK, new_peer_callback=None):

    # yields blocks until we run out
    loop = asyncio.get_event_loop()

    inv_batcher = InvBatcher()

    peer_to_header_q = create_peer_to_header_q(index_hash_work_tuples, inv_batcher)
    header_to_block_future_q = create_header_to_block_future_q(inv_batcher, input_q=peer_to_header_q)

    def got_addr(peer, name, data):
        pass

    async def got_new_peer(peer, q):
        install_pong_manager(peer)
        peer.set_request_callback("alert", lambda *args: None)
        peer.set_request_callback("addr", got_addr)
        peer.set_request_callback("inv", lambda *args: None)
        peer.set_request_callback("feefilter", lambda *args: None)
        peer.set_request_callback("sendheaders", lambda *args: None)
        peer.set_request_callback("sendcmpct", lambda *args: None)
        await peer_to_header_q.put(peer)
        await inv_batcher.add_peer(peer)
        if new_peer_callback:
            await new_peer_callback(peer)
        peer.start()

    peer_pipeline = get_peer_pipeline(network, peer_addresses, got_new_peer)

    new_peer_q = MappingQueue(
        dict(callback_f=got_new_peer, input_q=peer_pipeline, worker_count=1)
    )

    while True:
        v = loop.run_until_complete(header_to_block_future_q.get())
        if v is None:
            break
        block_future, index = v
        block = loop.run_until_complete(block_future)
        if hasattr(block, "tx_futures"):
            txs = []
            for f in block.tx_futures:
                txs.append(loop.run_until_complete(f))
            block.txs = txs
        yield block, index

    peer_to_header_q.stop()
    header_to_block_future_q.stop()
    new_peer_q.stop()
