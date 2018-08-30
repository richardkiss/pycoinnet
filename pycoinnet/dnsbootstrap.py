import asyncio

from .async_iterators import iter_to_aiter, map_aiter, flatten_aiter


def dns_bootstrap_host_port_iterator(network, getaddrinfo=asyncio.get_event_loop().getaddrinfo):

    hosts_seen = set()

    async def transformer(response_future):
        hosts = []
        for response in await response_future:
            host = response[-1][:2]
            if host not in hosts_seen:
                hosts_seen.add(host)
                hosts.append(host)
        return hosts

    getaddrinfo_futures = iter_to_aiter(
        asyncio.as_completed(
            [getaddrinfo(_, network.default_port) for _ in network.dns_bootstrap]))

    return flatten_aiter(map_aiter(getaddrinfo_futures, transformer))
