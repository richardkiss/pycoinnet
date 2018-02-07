import asyncio

from socket import gaierror

from .MappingQueue import MappingQueue


def dns_bootstrap_host_port_q(network, getaddrinfo=asyncio.get_event_loop().getaddrinfo):
    """
    Accepts network type and returns an asyncio.Queue, which is loads with tuples of the
    form (host, port). When it runs out, it puts a "None" to terminate.
    """

    async def getaddr(dns_host):
        return await getaddrinfo(dns_host, network.default_port)

    async def extract_pair(dns_response):
        return dns_response[-1][:2]

    hosts_seen = set()

    async def filter_host(host):
        if host in hosts_seen:
            return False
        hosts_seen.add(host)
        return True

    filters = [
        dict(flatten=True),
        dict(map_f=getaddr),
        dict(flatten=True),
        dict(map_f=extract_pair),
        dict(filter_f=filter_host),
    ]
    q = MappingQueue(*filters)
    q.put_nowait(network.dns_bootstrap)
    return q
