import binascii
import logging
import struct

import asyncio

from pycoin import encoding

MAGIC = binascii.unhexlify('f9beb4d9')


class BitcoinPeerStreamReader(asyncio.StreamReader):
    @asyncio.coroutine
    def readmessage(self):
        blob = yield from self.readexactly(len(MAGIC))
        if blob != MAGIC:
            s = "bad magic: got %s" % binascii.hexlify(blob)
            logging.error(s)
            raise Exception(s)
        message_name = (yield from self.readexactly(12)).replace(b"\0", b"")
        blob = yield from self.readexactly(4)
        size = int.from_bytes(blob, byteorder="little")
        transmitted_hash = yield from self.readexactly(4)

        message_data = yield from self.readexactly(size)
        actual_hash = encoding.double_sha256(message_data)[:4]
        if actual_hash == transmitted_hash:
            logging.debug("checksum is CORRECT")
        else:
            logging.error("checksum is WRONG: %s instead of %s", binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash))
        return message_name.decode("utf8"), message_data

    def packet_for_message(self, message_type, message_data):
        message_type = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = encoding.double_sha256(message_data)[:4]
        packet = b"".join([MAGIC, message_type, message_size, message_checksum, message_data])
        return packet
