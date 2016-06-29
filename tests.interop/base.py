
import asyncio
import logging
import os
import unittest

from pycoin.serialize import h2b_rev

from pycoinnet.Blockfetcher import Blockfetcher
from pycoinnet.InvFetcher import InvFetcher
from pycoinnet.Peer import Peer
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

LOG_FORMAT = ('%(asctime)s [%(process)d] [%(levelname)s] '
              '%(filename)s:%(lineno)d %(message)s')

asyncio.tasks._DEBUG = True
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
logging.getLogger("asyncio").setLevel(logging.INFO)


def log_file(logPath, level=logging.NOTSET):
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)


class InteropTest(unittest.TestCase):
    def setUp(self):
        try:
            host_port = os.getenv("BITCOIND_HOSTPORT")
            self.host, self.port = host_port.split(":")
            self.port = int(self.port)
        except Exception:
            raise ValueError('need to set BITCOIND_HOSTPORT="127.0.0.1:8333" for example')

    def test_connect(self):
        r, w = run(asyncio.open_connection(host=self.host, port=self.port))
        peer = Peer(r, w, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        peer.send_msg("version", **VERSION_MSG)
        msg = run(peer.next_message())
        assert msg[0] == 'version'
        peer.send_msg("verack")
        msg = run(peer.next_message())
        assert msg[0] == 'verack'
        peer.send_msg("mempool")
        msg_name, msg_data = run(peer.next_message())
        if msg_name == 'inv':
            items = msg_data.get("items")
            peer.send_msg("getdata", items=items)
            for _ in range(len(items)):
                msg_name, msg_data = run(peer.next_message())
                print(msg_data.get("tx"))

    def test_InvFetcher(self):
        BLOCK_95150_HASH = h2b_rev("00000000000026ace69f5cbe46f7bbe868737635edef3354ef09fdaad8c755fb")
        r, w = run(asyncio.open_connection(host=self.host, port=self.port))
        peer = Peer(r, w, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        inv_fetcher = InvFetcher(peer)
        peer.add_msg_handler(inv_fetcher.handle_msg)
        version_data = run(peer.perform_handshake(**VERSION_MSG))
        print(version_data)
        peer.start_dispatcher()
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
        r, w = run(asyncio.open_connection(host=self.host, port=self.port))
        peer = Peer(r, w, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        version_data = run(peer.perform_handshake(**VERSION_MSG))
        print(version_data)
        peer.start_dispatcher()
        hash_stop = b'\0' * 32
        block_locator_hashes = [hash_stop]
        peer.send_msg(message_name="getheaders",
                      version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = run(peer.wait_for_response('headers'))
        assert name == 'headers'
        print(data)

    def test_Blockfetcher_pair(self):
        blockfetcher = Blockfetcher()

        # create a connection
        r, w = run(asyncio.open_connection(host=self.host, port=self.port))
        peer = Peer(r, w, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
        version_data = run(peer.perform_handshake(**VERSION_MSG))
        print(version_data)
        peer.start_dispatcher()
        if 1:
            peer.add_msg_handler(blockfetcher.handle_msg)
            blockfetcher.add_peer(peer)

        # create a list of block headers we're interested in
        # use that to seed a list of blocks
        hash_stop = b'\0' * 32
        block_locator_hashes = [hash_stop]
        peer.send_msg(message_name="getheaders",
                      version=1, hashes=block_locator_hashes, hash_stop=hash_stop)
        name, data = run(peer.wait_for_response('headers'))
        assert name == 'headers'
        block_hashes = [bh.hash() for bh, v in data["headers"]]
            
        # create another connection
        if 1:
            r, w = run(asyncio.open_connection(host="144.76.86.66", port=self.port))
            peer = Peer(r, w, MAINNET.magic_header, MAINNET.parse_from_data, MAINNET.pack_from_data)
            version_data = run(peer.perform_handshake(**VERSION_MSG))
            print(version_data)
            peer.start_dispatcher()
            peer.add_msg_handler(blockfetcher.handle_msg)
            blockfetcher.add_peer(peer)

        futures = blockfetcher.fetch_blocks((bh, idx) for idx, bh in enumerate(block_hashes[:5000]))
        run(futures[499])
        peer.close()
        for idx, f in enumerate(futures):
            b = run(f)
            print("%s: %s" % (idx, b))
