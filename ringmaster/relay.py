import random
from collections import defaultdict

from zmq.eventloop import ioloop
from tornado import gen

# from circus.commands import get_commands
from circus.tornado.client import AsyncClient
from circus.stats.asyncclient import AsyncStatsConsumer
from circus.util import DEFAULT_ENDPOINT_DEALER

                                                                       # TAKE THAT, PEP8.


class Relay(object):
    """Strategies are defined per watcher.

    Here are examples of strategies, implemented by default:
    "cpu_bound", "memory_bound", "io_bound".
    """

    def __init__(self, nodes, loop=None):
        self.nodes = nodes

        self._clients = {}
        self._stats_consumers = {}
        self.loop = loop or ioloop.IOLoop.instance()
        self.stats_consumer = AsyncStatsConsumer('stat.', self.loop,
                                                 self.update_stats)

        self.loop.add_callback(self.watch_nodes_stats, self.nodes)
        self.last_stats = defaultdict(dict)

    def start(self):
        self.loop.start()

    @gen.coroutine
    def watch_nodes_stats(self, nodes):
        yield [gen.Task(self.watch_stats, fqdn) for fqdn in nodes]

    @gen.coroutine
    def watch_stats(self, fqdn):
        if fqdn not in self._stats_consumers:
            # get the stats endpoint from a running circusd
            client = self._get_client(fqdn)
            global_options = yield gen.Task(client.get_global_options)
            stats_endpoint = global_options['stats_endpoint']
            self.stats_consumer.connect(stats_endpoint)

    def update_stats(self, watcher, subtopic, stats):
        fqdn = stats.pop('fqdn')
        self.last_stats[fqdn] = stats
        self.loop.stop()

    def _get_client(self, fqdn):
        # XXX fallback if we don't manage to contact the client.
        if fqdn not in self._clients:
            endpoint = random.choice(list(self.nodes[fqdn]))
            self._clients[fqdn] = AsyncClient(self.loop,
                                              endpoint=endpoint)
        return self._clients[fqdn]


if __name__ == '__main__':

    list_of_nodes = {'yeah': set((DEFAULT_ENDPOINT_DEALER,))}
    relay = Relay(nodes=list_of_nodes)
    relay.start()
    print relay.last_stats
