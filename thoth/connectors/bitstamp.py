"""Bitstamp Websocket connector."""

import time
import logging
import json

from thoth.core.pusher import PusherConnector


log = logging.getLogger(__name__)


class BitstampConnector(PusherConnector):
    """Websocket Connector for the Pusher-based Bitstamp API."""

    def __init__(self, *args, **kwargs):
        """Initialize Connector."""
        pairs = 'btceur,eurusd,xrpusd,xrpeur,xrpbtc,ltcusd,ltceur,' \
                'ltcbtc,ethusd,etheur,ethbtc,'.split(',')
        super(BitstampConnector, self).__init__(pairs, *args, **kwargs)

    @staticmethod
    def extract_pair(actual_channel, expected_channel):
        """
        Split the actual_channel on expected_channel and return the resulting pair.

        :param expected_channel: str
        :param actual_channel: str
        :return: str
        """
        # This works because -for example -  splitting 'order_book' on 'order_book'
        # yields a list of 2 empty strings
        return actual_channel.split(expected_channel)[1].strip('_') or 'btcusd'

    # pylint: disable=unused-argument
    def _base_callback(self, *, pair, channels, data):
        """Put data on respective queue."""
        def ticker(data):
            """Put data on q with correct channel name."""
            decoded_data = json.loads(data)
            price, size = str(decoded_data['price']), str(decoded_data['amount'])
            side = decoded_data['type']
            uid = ("BUID:%s" % decoded_data['buy_order_id'] + "+" +
                   "SUID:%s" % decoded_data['sell_order_id'])
            ts = decoded_data['timestamp']
            trade_data = pair, price, size, side, uid, None, ts
            msg = pair, "Trades", [trade_data]
            self.q.put(msg)

        def book(data):
            """Put data on q with correct channel name."""
            unpacked_data = json.loads(data)
            ts = str(time.time())
            formatted_data = [[(price, size, ts) for price, size in unpacked_data['bids']],
                              [(price, size, ts) for price, size in unpacked_data['asks']], ts]

            msg = pair, "Book", formatted_data
            self.q.put(msg)

        def diff_book(data):
            """Put data on q with correct channel name."""
            msg = pair, "Raw", json.loads(data)
            self.q.put(msg)

        def orders_deleted(data):
            """Put data on q with correct channel name."""
            decoded_data = json.loads(data)
            price, _ = decoded_data['price'], decoded_data['amount']
            uid, ts = decoded_data['id'], decoded_data['datetime']
            side = 'ask' if int(decoded_data['order_type']) else 'bid'
            packed_data = pair, price, 0, side, uid, ts
            msg = pair, "Quotes", [packed_data]
            self.q.put(msg)

        def orders_changed(data):
            """Put data on q with correct channel name."""
            decoded_data = json.loads(data)
            price, size = decoded_data['price'], decoded_data['amount']
            uid, ts = decoded_data['id'], decoded_data['datetime']
            side = 'ask' if int(decoded_data['order_type']) else 'bid'
            packed_data = pair, price, size, side, uid, ts
            msg = pair, "Quotes", [packed_data]
            self.q.put(msg)

        def orders_created(data):
            """Put data on q with correct channel name."""
            decoded_data = json.loads(data)
            price, size = decoded_data['price'], decoded_data['amount']
            uid, ts = decoded_data['id'], decoded_data['datetime']
            side = 'ask' if int(decoded_data['order_type']) else 'bid'
            packed_data = pair, price, size, side, uid, ts
            msg = pair, "Quotes", [packed_data]
            self.q.put(msg)

        channel1 = self.subscribe(channels['ticker'])
        channel1.bind('trade', ticker)
        channel2 = self.subscribe(channels['book'])
        channel2.bind('data', book)
        channel3 = self.subscribe(channels['diff_book'])
        channel3.bind('data', diff_book)
        channel4 = self.subscribe(channels['orders'])
        channel4.bind('order_deleted', orders_deleted)
        channel4.bind('order_changed', orders_changed)
        channel4.bind('order_created', orders_created)

    def _pair_callback(self, data, pair=None):
        """Cal relevant function for given pair."""
        if pair:
            pair = '_' + pair

        channels = {'orders': 'live_orders%s' % pair,
                    'ticker': 'live_trades%s' % pair,
                    'book': 'order_book%s' % pair,
                    'diff_book': 'diff_order_book%s' % pair}
        self._base_callback(pair=pair[1:] if pair else 'btcusd', channels=channels, data=data)
