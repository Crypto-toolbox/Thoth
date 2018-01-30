"""Bitstamp Websocket connector."""

import time
import logging

from thoth.core.pusher import PusherConnector


log = logging.getLogger(__name__)


class BitstampConnector(PusherConnector):
    """Websocket Connector for the Pusher-based Bitstamp API."""
    channel = ['live_trades%s', 'order_book%s', 'diff_order_book%s', 'live_orders%s']

    def __init__(self, *args, **kwargs):
        """Initialize Connector."""
        pairs = 'btceur, eurusd, xrpusd, xrpeur, xrpbtc, ltcusd, ltceur, ltcbtc, ethusd, ' \
                'etheur, ethbtc, bchusd, bcheur, bchbtc'.strip(' ').split(',')
        super(BitstampConnector, self).__init__(pairs, *args, **kwargs)

    # pylint: disable=unused-argument
    def _base_callback(self, data, pair):
        """Put data on respective queue."""
        pair = '' if pair == 'btcusd' else '_' + pair

        trades, book, diff_book, orders = [chan % pair for chan in self.channels]

        def _handle_trades(data):
            """Put data on q with correct channel name."""
            topic = trades + '/data'
            self.push(topic, data, time.time())

        def _handle_book(data):
            topic = book + '/data'
            self.push(topic, data, time.time())

        def _handle_diff_book(data):
            topic = diff_book + '/data'
            self.push(topic, data, time.time())

        def _handle_orders_deleted(data):
            topic = orders + '/order_deleted'
            self.push(topic, data, time.time())

        def _handle_orders_changed(data):
            topic = orders + '/order_changed'
            self.push(topic, data, time.time())

        def _handle_orders_created(data):
            topic = orders + '/order_created'
            self.push(topic, data, time.time())

        channel1 = self.subscribe(trades)
        channel1.bind('trade', _handle_trades)
        channel2 = self.subscribe(book)
        channel2.bind('data', _handle_book)
        channel3 = self.subscribe(diff_book)
        channel3.bind('data', _handle_diff_book)
        channel4 = self.subscribe(orders)
        channel4.bind('order_deleted', _handle_orders_deleted)
        channel4.bind('order_changed', _handle_orders_changed)
        channel4.bind('order_created', _handle_orders_created)
