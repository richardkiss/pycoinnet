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


async def improve_headers(peer, bcv, inv_batcher):
    block_locator_hashes = bcv.block_locator_hashes()
    logging.debug("getting headers after %d", bcv.last_block_tuple()[0])
    data = await peer.request_response(
        "getheaders", "headers", version=1,
        hashes=block_locator_hashes, hash_stop=bcv.hash_initial_block())
    headers = [bh for bh, t in data["headers"]]

    if block_locator_hashes[-1] == bcv.hash_initial_block():
        # this hack is necessary because the stupid default client
        # does not send the genesis block!
        bh = headers[0].previous_block_hash
        f = await inv_batcher.inv_item_to_future(InvItem(ITEM_TYPE_BLOCK, bh))
        block = await f
        headers = [block] + headers
    return headers


def create_peer_to_block_pipe(bcv, inv_batcher, filter_f=lambda block_hash, index: ITEM_TYPE_BLOCK):
    """
    return a MappingQueue that accepts: peer objects
    and yields: block objects
    """

    improve_headers_pipe = asyncio.Queue()

    async def note_peer(peer, q):
        headers = await improve_headers(peer, bcv, inv_batcher)

        block_number = bcv.do_headers_improve_path(headers)
        if block_number is False:
            await q.put(None)
            return

        logging.debug("block header count is now %d", block_number)
        hashes = []

        for idx in range(bcv.last_block_index()+1-block_number):
            the_tuple = bcv.tuple_for_index(idx+block_number)
            assert the_tuple[0] == idx + block_number
            assert headers[idx].hash() == the_tuple[1]
            hashes.append(headers[idx])
        await q.put((block_number, hashes))
        await improve_headers_pipe.put(peer)

    async def create_block_hash_entry(item, q):
        if item is None:
            await q.put(None)
            return
        first_block_index, block_headers = item
        logging.info("got %d new header(s) starting at %d" % (len(block_headers), first_block_index))
        block_hash_priority_pair_list = [(bh, first_block_index + _) for _, bh in enumerate(block_headers)]

        for bh, pri in block_hash_priority_pair_list:
            item_type = filter_f(bh, pri)
            if not item_type:
                continue
            f = await inv_batcher.inv_item_to_future(InvItem(item_type, bh.hash()))
            await q.put((f, pri))

    async def wait_future(pair, q):
        if pair is None:
            await q.put(None)
            return
        future, index = pair
        block = await future
        await q.put((block, index))

    peer_to_block_pipe = MappingQueue(
        dict(callback_f=note_peer, worker_count=1, input_q=improve_headers_pipe),
        dict(callback_f=create_block_hash_entry, worker_count=1, input_q_maxsize=2),
        dict(callback_f=wait_future, input_q_maxsize=1000),
        final_q=asyncio.Queue(maxsize=100)
    )
    peer_to_block_pipe.inv_batcher = inv_batcher
    return peer_to_block_pipe


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
        network, index_hash_work_tuples, peer_addresses=None, filter_f=lambda bh, index: ITEM_TYPE_BLOCK):

    # yields blocks until we run out
    loop = asyncio.get_event_loop()

    blockchain_view = BlockChainView(index_hash_work_tuples)

    inv_batcher = InvBatcher()

    peer_to_block_pipe = create_peer_to_block_pipe(blockchain_view, inv_batcher, filter_f=filter_f)

    peer_pipeline = get_peer_pipeline(network, peer_addresses, peer_to_block_pipe)

    async def got_new_peer(peer, q):
        install_pong_manager(peer)
        peer.set_request_callback("alert", lambda *args: None)
        peer.set_request_callback("addr", lambda *args: None)
        peer.set_request_callback("inv", lambda *args: None)
        peer.set_request_callback("feefilter", lambda *args: None)
        await peer_to_block_pipe.put(peer)
        await inv_batcher.add_peer(peer)
        peer.start()

    new_peer_q = MappingQueue(
        dict(callback_f=got_new_peer, input_q=peer_pipeline, worker_count=1)
    )

    while True:
        v = loop.run_until_complete(peer_to_block_pipe.get())
        if v is None:
            break
        block, index = v
        if hasattr(block, "tx_futures"):
            txs = []
            for f in block.tx_futures:
                txs.append(loop.run_until_complete(f))
            block.txs = txs
        yield block, index

    peer_to_block_pipe.stop()
    new_peer_q.stop()
