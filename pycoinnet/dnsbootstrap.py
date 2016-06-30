import asyncio


@asyncio.coroutine
def _populate_queue(getaddr_futures, q):
    hosts_seen = set()
    for f in asyncio.as_completed(getaddr_futures):
        responses = yield from f
        for r in responses:
            pair = r[-1][:2]
            if pair in hosts_seen:
                continue
            hosts_seen.add(pair)
            q.put_nowait(pair)
    q.put_nowait(None)


def dns_bootstrap_host_port_q(network, getaddrinfo=asyncio.get_event_loop().getaddrinfo):
    """
    Accepts network type and returns an asyncio.Queue, which is loads with tuples of the
    form (host, port). When it runs out, it puts a "None" to terminate.
    """
    q = asyncio.Queue()
    dns_bootstrap = network.dns_bootstrap
    getaddr_futures = [getaddrinfo(h, network.default_port) for h in dns_bootstrap]
    asyncio.get_event_loop().create_task(_populate_queue(getaddr_futures, q))
    return q
