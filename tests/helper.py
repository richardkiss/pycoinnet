import hashlib

from pycoin import ecdsa
from pycoin.block import Block, BlockHeader
from pycoin.encoding import public_pair_to_sec
from pycoin.tx.Tx import Tx, TxIn, TxOut


GENESIS_TIME = 1390000000
DEFAULT_DIFFICULTY = 3000000
HASH_INITIAL_BLOCK = b'\0' * 32


def make_hash(i, s=b''):
    return hashlib.sha256(("%d_%s" % (i, s)).encode()).digest()


def make_tx(i):
    txs_in = [TxIn(make_hash(i*10000+idx), (i+idx) % 2) for idx in range(3)]
    txs_out = [TxOut(i*40000, make_hash(i*20000+idx)) for idx in range(2)]
    tx = Tx(1, txs_in, txs_out)
    return tx


def make_headers(count, header=None):
    if header is None:
        last_hash = HASH_INITIAL_BLOCK
    else:
        last_hash = header.hash()
    tweak = last_hash
    headers = []
    for i in range(count):
        headers.append(
            BlockHeader(version=1, previous_block_hash=last_hash, merkle_root=make_hash(i, tweak),
                        timestamp=GENESIS_TIME+i*600, difficulty=DEFAULT_DIFFICULTY, nonce=i*137))
        last_hash = headers[-1].hash()
    return headers


def make_block(index):
    s = index*30000
    txs = [make_tx(i) for i in range(s, s+8)]
    block = Block(version=1, previous_block_hash=b'\0'*32, merkle_root=b'\0'*32,
                  timestamp=GENESIS_TIME+index, difficulty=s, nonce=s, txs=txs)
    return block


def coinbase_tx(secret_exponent):
    public_pair = ecdsa.public_pair_for_secret_exponent(
        ecdsa.secp256k1.generator_secp256k1, secret_exponent)
    public_key_sec = public_pair_to_sec(public_pair)
    return Tx.coinbase_tx(public_key_sec, 2500000000)

COINBASE_TX = coinbase_tx(1)


def make_blocks(count, nonce_base=30000, previous_block_hash=HASH_INITIAL_BLOCK):
    blocks = []
    for i in range(count):
        s = i * nonce_base
        txs = [COINBASE_TX]  # + [make_tx(i) for i in range(s, s+8)]
        nonce = s
        while True:
            block = Block(version=1, previous_block_hash=previous_block_hash, merkle_root=b'\0'*32,
                          timestamp=GENESIS_TIME+i*600, difficulty=i, nonce=nonce, txs=txs)
            if block.hash()[-1] == i & 0xff:
                break
            nonce += 1
        blocks.append(block)
        previous_block_hash = block.hash()
    return blocks
