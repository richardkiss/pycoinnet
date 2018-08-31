import asyncio
import logging
import os.path

from pycoinnet.aitertools import map_aiter
from pycoinnet.BlockChainView import BlockChainView
from pycoinnet.PeerManager import PeerManager
from pycoinnet.peer_pipeline import get_peer_iterator
from pycoinnet.pong_manager import install_pong_manager


LOG_FORMAT = '%(asctime)s [%(process)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s'


def init_logging(level=logging.NOTSET, asyncio_debug=False):
    asyncio.tasks._DEBUG = asyncio_debug
    logging.basicConfig(level=level, format=LOG_FORMAT)
    logging.getLogger("asyncio").setLevel(logging.DEBUG if asyncio_debug else logging.INFO)


def set_log_file(logPath, level=logging.NOTSET):
    if logPath is None:
        return
    new_log = logging.FileHandler(logPath)
    new_log.setLevel(level)
    new_log.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(new_log)


def storage_base_path():
    p = os.path.expanduser("~/.pycoinnet/default/")
    if not os.path.exists(p):
        os.makedirs(p)
    return p


def get_current_view(path):
    try:
        with open(path) as f:
            return BlockChainView.from_json(f.read())
    except FileNotFoundError:
        pass
    return BlockChainView()


def save_bcv(path, bcv):
    json = bcv.as_json(sort_keys=True, indent=2)
    tmp = "%s.tmp" % path
    with open(tmp, "w") as f:
        f.write(json)
    os.rename(tmp, path)


def peer_manager_for_args(args, bloom_filter=None):

    peer_iterator = get_peer_iterator(args.network, args.peer)

    if bloom_filter:
        filter_bytes, hash_function_count, tweak = bloom_filter.filter_load_params()
        flags = 1  # BLOOM_UPDATE_ALL = 1  # BRAIN DAMAGE

        async def got_new_peer(peer):
            if args.spv:
                peer.send_msg("filterload", filter=filter_bytes, tweak=tweak,
                              hash_function_count=hash_function_count, flags=flags)
            return peer

        peer_iterator = map_aiter(got_new_peer, peer_iterator)

    peer_manager = PeerManager(peer_iterator, getattr(args, "count", 4))
    install_pong_manager(peer_manager)
    return peer_manager
