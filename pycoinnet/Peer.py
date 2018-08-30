import asyncio
import binascii
import logging
import struct

from pycoin.encoding import hash


class ProtocolError(Exception):
    pass


class Peer:
    DEFAULT_MAX_MSG_SIZE = 2*1024*1024

    def __init__(self, stream_reader, stream_writer, magic_header,
                 parse_from_data_f, pack_from_data_f, max_msg_size=DEFAULT_MAX_MSG_SIZE):
        self._reader = stream_reader
        self._writer = stream_writer
        self._magic_header = magic_header
        self._parse_from_data = parse_from_data_f
        self._pack_from_data = pack_from_data_f
        self._max_msg_size = max_msg_size
        self._msg_lock = asyncio.Lock()
        # events
        self._is_closed = asyncio.Future()
        # stats
        self._bytes_read = 0
        self._bytes_writ = 0

    def send_msg(self, message_name, **kwargs):
        message_data = self._pack_from_data(message_name, **kwargs)
        message_type = message_name.encode("utf8")
        message_type_padded = (message_type+(b'\0'*12))[:12]
        message_size = struct.pack("<L", len(message_data))
        message_checksum = hash.double_sha256(message_data)[:4]
        packet = b"".join([
            self._magic_header, message_type_padded, message_size, message_checksum, message_data
        ])
        logging.debug("sending message %s [%d bytes] to %s", message_type.decode("utf8"), len(packet), self)
        self._bytes_writ += len(packet)
        self._writer.write(packet)

    async def next_message(self, unpack_to_dict=True):
        header_size = len(self._magic_header)
        try:
            async with self._msg_lock:
                # read magic header
                reader = self._reader
                blob = await reader.readexactly(header_size)
                self._bytes_read += header_size
                if blob != self._magic_header:
                    logging.error("got bad magic: %s" % binascii.hexlify(blob))
                    return None

                # read message name
                message_size_hash_bytes = await reader.readexactly(20)
                self._bytes_read += 20
                message_name_bytes = message_size_hash_bytes[:12]
                message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

                # get size of message
                size_bytes = message_size_hash_bytes[12:16]
                size = int.from_bytes(size_bytes, byteorder="little")
                if size > self._max_msg_size:
                    logging.error("message too large: size %d" % size)
                    return None

                # read the hash, then the message
                transmitted_hash = message_size_hash_bytes[16:20]
                message_data = await reader.readexactly(size)
                self._bytes_read += size
        except asyncio.streams.IncompleteReadError:
            return None

        # check the hash
        actual_hash = hash.double_sha256(message_data)[:4]
        if actual_hash != transmitted_hash:
            logging.error("checksum is WRONG: %s instead of %s" % (
                binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash)))
            return None
        logging.debug("message %s: %s (%d byte payload)", self, message_name, len(message_data))
        if unpack_to_dict:
            message_data = self._parse_from_data(message_name, message_data)
        return message_name, message_data

    def write_eof(self):
        self._writer.write_eof()

    def close(self):
        self._is_closed.set_result(None)
        self._writer.close()

    def is_closing(self):
        return self._writer._transport.is_closing()

    def peername(self):
        return self._reader._transport.get_extra_info("peername") or self._reader._transport

    async def wait_until_close(self):
        await self._is_closed

    def __repr__(self):
        return "<Peer %s>" % str(self.peername())
