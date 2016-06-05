
import asyncio
import os
import unittest

from pycoin.serialize import h2b_rev

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.Dispatcher import Dispatcher
from pycoinnet.InvFetcher import InvFetcher
from pycoinnet.PeerProtocol import PeerProtocol
from pycoinnet.msg.InvItem import InvItem, ITEM_TYPE_BLOCK, ITEM_TYPE_MERKLEBLOCK, ITEM_TYPE_TX
from pycoinnet.msg.PeerAddress import PeerAddress
from pycoinnet.networks import MAINNET


def run(f):
    return asyncio.get_event_loop().run_until_complete(f)


VERSION_MSG = dict(
    version=70001, subversion=b"/Notoshi/", services=1, timestamp=1392760610,
    remote_address=PeerAddress(1, "127.0.0.2", 6111),
    local_address=PeerAddress(1, "127.0.0.1", 6111),
    nonce=3412075413544046060,
    last_block_index=10000
)


class InteropTest(unittest.TestCase):
    def setUp(self):
        try:
            host_port = os.getenv("BITCOIND_HOSTPORT")
            self.host, self.port = host_port.split(":")
            self.port = int(self.port)
        except Exception:
            raise ValueError('need to set BITCOIND_HOSTPORT="127.0.0.1:8333" for example')

    def test_connect(self):
        loop = asyncio.get_event_loop()
        transport, protocol = run(loop.create_connection(
            lambda: PeerProtocol(MAINNET), host=self.host, port=self.port))
        protocol.send_msg("version", **VERSION_MSG)
        msg = run(protocol.next_message())
        assert msg[0] == 'version'
        protocol.send_msg("verack")
        msg = run(protocol.next_message())
        assert msg[0] == 'verack'
        protocol.send_msg("mempool")
        msg_name, msg_data = run(protocol.next_message())
        if msg_name == 'inv':
            items = msg_data.get("items")
            protocol.send_msg("getdata", items=items)
            for _ in range(len(items)):
                msg_name, msg_data = run(protocol.next_message())
                print(msg_data.get("tx"))

    def test_InvFetcher(self):
        BLOCK_95150_HASH = h2b_rev("00000000000026ace69f5cbe46f7bbe868737635edef3354ef09fdaad8c755fb")
        loop = asyncio.get_event_loop()
        transport, protocol = run(loop.create_connection(
            lambda: PeerProtocol(MAINNET), host=self.host, port=self.port))
        inv_fetcher = InvFetcher(protocol)
        dispatcher = Dispatcher(protocol)
        dispatcher.add_msg_handler(inv_fetcher.handle_msg)
        version_data = run(dispatcher.handshake(**VERSION_MSG))
        print(version_data)
        asyncio.get_event_loop().create_task(dispatcher.dispatch_messages())
        inv_item = InvItem(ITEM_TYPE_BLOCK, BLOCK_95150_HASH)
        bl = run(inv_fetcher.fetch(inv_item))
        assert len(bl.txs) == 5

        inv_item = InvItem(ITEM_TYPE_MERKLEBLOCK, BLOCK_95150_HASH)
        mb = run(inv_fetcher.fetch(inv_item))
        txs = [run(f) for f in mb.tx_futures]
        assert len(txs) == 5
        for tx1, tx2 in zip(txs, bl.txs):
            assert tx1.id() == tx2.id()

        # test "notfound"
        inv_item = InvItem(ITEM_TYPE_TX, h2b_rev("f"*64))
        b = run(inv_fetcher.fetch(inv_item))
        assert b is None

    def test_headers_catchup(self):
        loop = asyncio.get_event_loop()
        transport, protocol = run(loop.create_connection(
            lambda: PeerProtocol(MAINNET), host=self.host, port=self.port))
        dispatcher = Dispatcher(protocol)
        version_data = run(dispatcher.handshake(**VERSION_MSG))
        print(version_data)
        asyncio.get_event_loop().create_task(dispatcher.dispatch_messages())
        hash_stop = b'\0' * 32
        block_locator_hashes = [hash_stop]
        protocol.send_msg(message_name="getheaders",
                          version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = run(dispatcher.wait_for_response('headers'))
        assert name == 'headers'
        print(data)

    def test_Blockfetcher(self):
        loop = asyncio.get_event_loop()
        transport, protocol = run(loop.create_connection(
            lambda: PeerProtocol(MAINNET), host=self.host, port=self.port))
        dispatcher = Dispatcher(protocol)
        run(dispatcher.handshake(**VERSION_MSG))
        asyncio.get_event_loop().create_task(dispatcher.dispatch_messages())
        # create a list of block headers we're interested in
        # use that to seed a list of blocks
        hash_stop = b'\0' * 32
        block_locator_hashes = [hash_stop]
        protocol.send_msg(message_name="getheaders",
                          version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = run(dispatcher.wait_for_response('headers'))
        assert name == 'headers'
        block_hashes = [bh.hash() for bh, v in data["headers"]]
        blockfetcher = Blockfetcher()
        inv_fetcher = InvFetcher(protocol)
        dispatcher.add_msg_handler(inv_fetcher.handle_msg)
        blockfetcher.add_fetcher(inv_fetcher)
        futures = [blockfetcher.get_block_future(bh, idx) for idx, bh in enumerate(block_hashes[:5000])]
        for f in futures:
            b = run(f)
            print(b)
