import asyncio
import logging


class PeerManager:
    # TODO: handle incoming
    def __init__(self, peer_pipeline, desired_peer_count, new_peer_async_callback=lambda peer: None):
        self._peer_pipeline = peer_pipeline
        self._desired_peer_count = desired_peer_count
        self._new_peer_async_callback = new_peer_async_callback
        self._peers = set()
        self._event_callback_index = 0
        self._event_callbacks = {}  # BRAIN DAMAGE: weakref.WeakValueDictionary()
        self._is_running = True
        self._outgoing_coroutines = [self._maintain_outgoing() for _ in range(desired_peer_count)]
        self._incoming_peers = asyncio.Queue()
        self._maintain_task = asyncio.gather(
            asyncio.gather(*self._outgoing_coroutines), self._maintain_incoming())

    def peers(self):
        return list(self._peers)

    def accept_incoming_peer(self, peer):
        # TODO: handle incoming
        # self._incoming_peers.put_nowait(peer)
        pass

    def close_all(self):
        self._is_running = False
        for peer in self._peers:
            peer.close()

    def __del__(self):
        self.close_all()

    async def _peer_lifecycle(self, peer):
        await self._new_peer_async_callback(peer)
        self._peers.add(peer)
        # tell children
        for callback in list(self._event_callbacks.values()):
            asyncio.get_event_loop().call_soon(callback, peer, None, None)
        await self.process_events(peer)
        peer.close()
        logging.debug("peer closed %s", peer)
        self._peers.remove(peer)

    async def _maintain_outgoing(self):
        while self._is_running:
            peer = await self._peer_pipeline.get()
            await self._peer_lifecycle(peer)
            logging.debug("acquiring new peer to replace %s", peer)

    async def _maintain_incoming(self):
        return
        # TODO: fix this
        while self._is_running:
            peer = await self._incoming_peers.get()
            # we can interrupt here, and that's not good
            self._peer_lifecycle(peer)

    def add_event_callback(self, callback):
        self._event_callback_index += 1
        self._event_callbacks[self._event_callback_index] = callback
        for peer in list(self._peers):
            asyncio.get_event_loop().call_soon(callback, peer, None, None)
        return self._event_callback_index

    def del_event_callback(self, handle):
        if handle in self._event_callbacks:
            del self._event_callbacks[handle]

    async def process_events(self, peer):
        while True:
            event = await peer.next_message()
            if event is None:
                break
            name, data = event
            for callback in list(self._event_callbacks.values()):
                asyncio.get_event_loop().call_soon(callback, peer, name, data)
