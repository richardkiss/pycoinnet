from pycoin.block import Block
from pycoin.serialize import bitcoin_streamer
from pycoin.tx.Tx import Tx

from .InvItem import InvItem
from .PeerAddress import PeerAddress


def init_bitcoin_streamer():

    EXTRA_PARSING = [
        ("A", (PeerAddress.parse, lambda f, peer_addr: peer_addr.stream(f))),
        ("v", (InvItem.parse, lambda f, inv_item: inv_item.stream(f))),
        ("T", (Tx.parse, lambda f, tx: tx.stream(f))),
        ("B", (Block.parse, lambda f, block: block.stream(f))),
    ]

    bitcoin_streamer.BITCOIN_STREAMER.register_functions(EXTRA_PARSING)
