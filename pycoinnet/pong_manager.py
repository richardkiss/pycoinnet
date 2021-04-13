def install_pong_manager(peer):
    def pong_callback(peer, name, data):
        peer.send_msg("pong", nonce=data["nonce"])
    peer.set_request_callback("ping", pong_callback)
