#!/usr/bin/env python

import binascii

from collections import namedtuple

from pycoin.block import Block, BlockHeader
from pycoin.message.make_parser_and_packer import (
    make_parser_and_packer, standard_messages,
    standard_message_post_unpacks, standard_streamer, standard_parsing_functions
)
from pycoin.tx.Tx import Tx

Network = namedtuple(
    'Network', (
        'code',
        'magic_header', 'dns_bootstrap', 'default_port', 'pack_from_data',
        'parse_from_data'
    )
)

streamer = standard_streamer(standard_parsing_functions(Block, BlockHeader, Tx))
btc_parser, btc_packer = make_parser_and_packer(
    streamer, standard_messages(), standard_message_post_unpacks(streamer))


MAINNET = Network(
    'BTC', binascii.unhexlify('F9BEB4D9'), [
        "seed.bitcoin.sipa.be", "dnsseed.bitcoin.dashjr.org",
        "bitseed.xf2.org", "dnsseed.bluematt.me",
    ],
    8333,
    btc_packer,
    btc_parser,
)

TESTNET = Network(
    'XTC', binascii.unhexlify('0B110907'), [
        "bitcoin.petertodd.org", "testnet-seed.bitcoin.petertodd.org",
        "bluematt.me", "testnet-seed.bluematt.me"
    ],
    18333,
    btc_packer,
    btc_parser,
)


NETWORKS = [MAINNET, TESTNET]
