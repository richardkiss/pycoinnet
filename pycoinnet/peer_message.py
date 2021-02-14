import asyncio
import binascii
import logging

from pycoin.encoding import hash


def make_reader_to_peer_stream(magic_header):

    def reader_to_peer_stream(rw_event):
        """
        Turn a reader into a generator that yields bitcoin peer messages.
        """
        reader, writer = rw_event
        header_size = len(magic_header)
        max_msg_size = 2 * 1024 * 1024

        async def event_stream():
            while True:
                try:
                    # read magic header
                    blob = await reader.readexactly(header_size)
                    if blob != magic_header:
                        logging.error("got bad magic header %s from %s" % (binascii.hexlify(blob), reader))
                        break

                    # read message name
                    message_size_hash_bytes = await reader.readexactly(20)
                    message_name_bytes = message_size_hash_bytes[:12]
                    message_name = message_name_bytes.replace(b"\0", b"").decode("utf8")

                    # get size of message
                    size_bytes = message_size_hash_bytes[12:16]
                    size = int.from_bytes(size_bytes, byteorder="little")
                    if size > max_msg_size:
                        logging.error("message size %d too large from %s" % (size, reader))
                        break

                    # read the hash, then the message
                    transmitted_hash = message_size_hash_bytes[16:20]
                    message_data = await reader.readexactly(size)

                    # check the hash
                    actual_hash = hash.double_sha256(message_data)[:4]
                    if actual_hash != transmitted_hash:
                        logging.error("checksum is WRONG: %s instead of %s" % (
                            binascii.hexlify(actual_hash), binascii.hexlify(transmitted_hash)))
                        break

                    logging.debug("message %s (%d byte payload)", message_name, len(message_data))
                    yield dict(name=message_name, data=message_data, writer=writer)

                except asyncio.IncompleteReadError:
                    break

        return event_stream()

    return reader_to_peer_stream
