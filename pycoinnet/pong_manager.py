def install_pong_manager(peer_manager):
    peer_manager.add_event_callback(pong_callback)


def pong_callback(peer, name, data):
    if name == "ping":
        peer.send_msg("pong", nonce=data["nonce"])
