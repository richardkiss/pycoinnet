import asyncio


def create_pong_task(peer_manager):

    async def task():
        async for peer, name, data in peer_manager.new_event_aiter():
            if name == "ping":
                peer.send_msg("pong", nonce=data["nonce"])

    return asyncio.ensure_future(task())
