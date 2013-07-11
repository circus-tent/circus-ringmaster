import random
from collections import defaultdict

import zmq
from zmq.eventloop import ioloop
from tornado import gen

from circus.async.client import AsyncClient
from circus.stats.asyncclient import AsyncStatsConsumer
from circus.util import DEFAULT_ENDPOINT_DEALER
from circus.controller import BaseController
from circus.commands import ok


class Relay(BaseController):
    """

    A relay object listens on the zmq port and relays its commands to one or
    more nodes, depending the strategy used.

    Strategies are defined per watcher, and can be (for now) one of the ones
    defined by the strategies package.
    "cpu_bound", "memory_bound", "io_bound".
    """

    def __init__(self, nodes, strategies, endpoint=DEFAULT_ENDPOINT_DEALER,
                 loop=None, context=None):
        self._clients = {}
        self._stats_consumers = {}

        self.nodes = nodes
        self.strategies = strategies
        self.endpoint = endpoint
        self.last_stats = defaultdict(dict)
        self.loop = loop or ioloop.IOLoop.instance()
        self.context = context or zmq.Context.instance()

        # This is better than super, let's call it "awesome".
        BaseController.__init__(self, self.endpoint, self.context, self.loop)

        print "Waiting for commands on %s" % self.endpoint

        # Call update_stats each time we get data from the stats.
        self.stats_consumer = AsyncStatsConsumer('stat.', self.loop,
                                                 self.update_stats)
        # Start to watch the stats for all the nodes.
        self.loop.add_callback(self.watch_nodes_stats, self.nodes)

    def start(self):
        BaseController.start(self)
        self.loop.start()

    def stop(self):
        BaseController.stop(self)
        self.loop.stop()

    @gen.coroutine
    def watch_nodes_stats(self, nodes):
        yield [gen.Task(self.watch_stats, fqdn) for fqdn in nodes]

    @gen.coroutine
    def watch_stats(self, fqdn):
        if fqdn not in self._stats_consumers:
            # Get the stats endpoint from a running circusd.
            client = self._get_client(fqdn)
            global_options = yield gen.Task(client.get_global_options)
            stats_endpoint = global_options['stats_endpoint']
            self.stats_consumer.connect(stats_endpoint)

    def update_stats(self, watcher, subtopic, stats):
        # Only store the last stats for now.
        fqdn = stats.pop('fqdn')
        self.last_stats[fqdn] = stats

    @gen.coroutine
    def execute_command(self, cmd_name, properties, cid, msg, cast):
        targets = [self._get_client(f) for f in self.nodes]
        if cmd_name in ('status',):
            results = yield [gen.Task(t.send_message, cmd_name, **properties)
                             for t in targets]
            print dict(zip(self.nodes, results))
            # self.send_response(cid, msg, dict(zip(self.nodes, results)))
            raise gen.Return(self.send_ok(cid, msg))

    def _get_client(self, fqdn):
        # XXX fallback if we don't manage to establish a connection with the
        # client.
        if fqdn not in self._clients:
            endpoint = random.choice(list(self.nodes[fqdn]))
            self._clients[fqdn] = AsyncClient(self.loop,
                                              endpoint=endpoint)
        return self._clients[fqdn]

    send_response = gen.coroutine(BaseController.send_response)

if __name__ == '__main__':

    relay = Relay(nodes={'yeah': set((DEFAULT_ENDPOINT_DEALER,))},
                  strategies={'*': 'cpu_bound'},
                  endpoint='tcp://127.0.0.1:6555')
    try:
        relay.start()
    except KeyboardInterrupt:
        relay.stop()
    print relay.last_stats
