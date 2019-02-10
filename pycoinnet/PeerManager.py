import asyncio

from aiter import aiter_forker, iter_to_aiter, join_aiters, map_aiter


def event_aiter_from_peer_aiter(peer_aiter):
    async def peer_to_events(peer):
        async def add_peer(event):
            name, data = event
            return peer, name, data
        return map_aiter(add_peer, peer.event_aiter())
    return join_aiters(map_aiter(peer_to_events, peer_aiter))


class PeerManager:
    def __init__(self, peer_aiter, host_gate_aiter):
        self._host_gate_aiter = host_gate_aiter
        self._active_peers = set()
        self._peer_aiter_forker = aiter_forker(peer_aiter)
        self._event_aiter_forker = aiter_forker(event_aiter_from_peer_aiter(self.new_peer_aiter()))
        self._watcher_task = asyncio.ensure_future(self._watcher())

    async def _watcher(self):
        peer_aiter = self.new_peer_aiter(is_active=False)
        async for peer in peer_aiter:
            self._active_peers.add(peer)
        for peer in list(self._active_peers):
            await peer.wait_until_close()

    async def close_all(self):
        self._host_gate_aiter.stop()
        async for peer in self.new_peer_aiter():
            peer.close()
        await self._watcher_task

    def new_peer_aiter(self, is_active=True):
        return join_aiters(iter_to_aiter([
            self.active_peers_aiter(),
            self._peer_aiter_forker.fork(is_active=is_active)]))

    def new_event_aiter(self, is_active=True):
        return self._event_aiter_forker.fork(is_active=is_active)

    def active_peers_aiter(self):
        return iter_to_aiter([_ for _ in self._active_peers if not _.is_closing()])
