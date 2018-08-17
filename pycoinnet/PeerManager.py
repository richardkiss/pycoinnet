import asyncio
import logging
import weakref


class PeerManager:
    # TODO: handle incoming
    def __init__(self, peer_pipeline, desired_peer_count, new_peer_async_callback=lambda peer: None):
        self._peer_pipeline = peer_pipeline
        self._desired_peer_count = desired_peer_count
        self._new_peer_async_callback = new_peer_async_callback
        self._peers = set()
        self._pipelines = weakref.WeakSet()
        self._is_running = True
        self._outgoing_coroutines = [self._maintain_outgoing() for _ in range(desired_peer_count)]
        self._incoming_peers = asyncio.Queue()
        self._maintain_task = asyncio.gather(
            asyncio.gather(*self._outgoing_coroutines), self._maintain_incoming())

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
        for pipeline in list(self._pipelines):
            if not pipeline.full():
                pipeline.put_nowait(peer)
        await peer.wait_until_close()
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

    def new_peer_pipeline(self, pipeline=None):
        if pipeline is None:
            pipeline = asyncio.Queue()
        self._pipelines.add(pipeline)
        for peer in list(self._peers):
            pipeline.put_nowait(peer)
        return pipeline
