"""Poloniex Connector which pre-formats incoming data to the CTS standard."""

import logging
import json
import time

from thoth.connectors.base import WebsocketConnector


log = logging.getLogger(__name__)


class GDAXConnector(WebsocketConnector):
    """Class to pre-process HitBTC data, before passing it up to a Node."""
    channels = ['level2', 'heartbeat', 'ticker', 'full']

    def __init__(self, pairs, **conn_ops):
        """Initialize a PoloniexConnector instance."""
        url = 'wss://ws-feed.gdax.com'
        super(GDAXConnector, self).__init__(url, **conn_ops)
        self.pairs = pairs
        self.session_sequence = 0

    def _on_open(self, ws):
        """Send subscription on open connection."""
        super(GDAXConnector, self)._on_open(ws)
        subscription_request = {'type': 'subscribe', 'product_ids': self.pairs,
                                'channels': self.channels}
        self.send(subscription_request)

    def _on_message(self, ws, data):
        message = json.loads(data)
        mtype = message['type']
        if mtype in ('snapshot', 'l2update'):
            topic = 'order_book_' + data['product_id'] + '/' + data['type']
        elif mtype in ('ticker',):
            topic = 'ticker_' + data['product_id']
        elif mtype in ('match','received', 'open', 'done', 'change', 'margin_profile_update',
                       'activate'):
            topic = 'full_order_book_' + data['product_id'] + '/' + data['type']
        elif mtype == 'heartbeat':
            topic = 'heartbeat_' + data['product_id']
        elif mtype == 'subscription':
            log.debug(message)
            return
        else:
            log.error(message)
            return
        super(GDAXConnector, self)._on_message(ws, (topic, data, time.time()))

    def _matches(self, data, ts):
        topic = 'matches_' + data['product_id']
        self.push(topic, data, ts)

    def _full(self, data, ts):
        topic = 'full_order_book_' + data['product_id'] + '/' + data['type']
        self.push(topic, data, ts)

    def _hb(self, data, ts):

        self.push(topic, data, ts)

