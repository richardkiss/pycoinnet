
import asyncio


def dns_bootstrap_host_port_lists(network, count, getaddrinfo=asyncio.get_event_loop().getaddrinfo):
    """
    Accepts network type and returns a list of futures, each of which produces a
    (host, port) tuple. If you ask for too many, the last few will be None.
    """
    dns_bootstrap = network.dns_bootstrap
    getaddr_futures = [getaddrinfo(h, network.default_port) for h in dns_bootstrap]
    futures = [asyncio.Future() for _ in range(count)]

    @asyncio.coroutine
    def populate_futures():
        hosts_seen = set()
        idx = 0
        for f in asyncio.as_completed(getaddr_futures):
            responses = yield from f
            for r in responses:
                pair = r[-1][:2]
                if pair in hosts_seen:
                    continue
                hosts_seen.add(pair)
                futures[idx].set_result(pair)
                idx += 1
                if idx >= count:
                    return
        while idx < count:
            futures[idx].set_result(None)
            idx += 1

    asyncio.get_event_loop().create_task(populate_futures())
    return futures
