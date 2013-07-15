import json
import random
from collections import defaultdict, OrderedDict

import zmq
from zmq.eventloop import ioloop
from tornado import gen

from circus import logger
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
    def handle_message(self, raw_msg):
        cid, msg = raw_msg
        msg = msg.strip()

        if not msg:
            self.send_response(cid, msg, "error: empty command")
        else:
            logger.debug("got message %s", msg)
            try:
                yield gen.Task(self.dispatch, cid, msg)
            except Exception as e:
                print str(e)
                self.send_error(cid, msg, 'Handle Error: %s' % e)

    @gen.coroutine
    def dispatch(self, cid, msg):
        try:
            json_msg = json.loads(msg)
        except ValueError:
            raise gen.Return(self.send_error(cid, msg, "json invalid",
                                             errno=errors.INVALID_JSON))

        cmd_name = json_msg.get('command')
        properties = json_msg.get('properties', {})
        cast = json_msg.get('msg_type') == "cast"

        resp = yield gen.Task(self.execute_command, cmd_name, properties, cid, msg, cast)

        if resp is None:
            resp = ok()

        if not isinstance(resp, (dict, list,)):
            msg = "msg %r tried to send a non-dict: %s" % (msg, str(resp))
            logger.error("msg %r tried to send a non-dict: %s", msg, str(resp))
            raise gen.Return(self.send_error(cid, msg, "server error", cast=cast,
                                             errno=errors.BAD_MSG_DATA_ERROR))

        if isinstance(resp, list):
            resp = {"results": resp}

        self.send_ok(cid, msg, resp, cast=cast)

    @gen.coroutine
    def execute_command(self, cmd_name, properties, cid, msg, cast):
        targets = [self._get_client(f) for f in self.nodes]
        if cmd_name in ('status',):
            results = yield [gen.Task(t.send_message, cmd_name, **properties)
                             for t in targets]
            
            answer = OrderedDict()
            for i, node in enumerate(self.nodes):
                for key, value in results[i]['statuses'].iteritems():
                    answer['%s:%s' % (node, key)] = value
            raise gen.Return(dict(statuses=answer))

    def _get_client(self, fqdn):
        # XXX fallback if we don't manage to establish a connection with the
        # client.
        if fqdn not in self._clients:
            endpoint = random.choice(list(self.nodes[fqdn]))
            self._clients[fqdn] = AsyncClient(self.loop,
                                              endpoint=endpoint)
        return self._clients[fqdn]


if __name__ == '__main__':

    relay = Relay(nodes={'yeah': set((DEFAULT_ENDPOINT_DEALER,))},
                  strategies={'*': 'cpu_bound'},
                  endpoint='tcp://127.0.0.1:6555')
    try:
        relay.start()
    except KeyboardInterrupt:
        relay.stop()
    print relay.last_stats
