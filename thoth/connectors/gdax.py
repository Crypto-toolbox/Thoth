"""Poloniex Connector which pre-formats incoming data to the CTS standard."""

import logging
from threading import Timer


from thoth.connectors.base import WebSocketConnectorThread


log = logging.getLogger(__name__)


class GDAXConnector(WebSocketConnectorThread):
    """Class to pre-process HitBTC data, before passing it up to a Node."""

    def __init__(self, **conn_ops):
        """Initialize a PoloniexConnector instance."""
        url = 'wss://ws-feed.gdax.com'
        super(GDAXConnector, self).__init__(url, **conn_ops)
        self._msg_handler = {'error': self._handle_error,
                             'subscriptions': self._handle_subscriptions,
                             'snapshot': self._handle_book,
                             'l2update': self._handle_book,
                             'received': self._handle_raw_book,
                             'open': self._handle_raw_book,
                             'done': self._handle_raw_book,
                             'match': self._handle_raw_book,
                             'change': self._handle_raw_book}
        self.session_sequence = 0

    def _start_timers(self):
        """Reset and start timers for API connection."""
        self._stop_timers()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def send_ping(self):
        """Override ping method."""
        return

    def _on_open(self, ws):
        """Send subscription on open connection."""
        super(GDAXConnector, self)._on_open(ws)
        subscription_request = {'type': 'subscribe', 'product_ids': ['ETH-BTC'],
                                'channels': ['level2']}
        self.send(subscription_request)

    def _stop_timers(self):
        """Stop connection timer."""
        if self.connection_timer:
            self.connection_timer.cancel()

    def pass_up(self, data, recv_at):
        """Process the data and put it on the internal Queue."""
        dtype = data['type']
        recv_at = str(recv_at)
        if dtype == 'error':
            raise ValueError(data)
        elif dtype == 'subscriptions':
            return
        elif dtype in ('l2update', 'snapshot'):
            if dtype == 'snapshot':
                self._handle_book(data, recv_at)
            else:
                self._handle_quotes(data, recv_at)

    # pylint: disable=unused-argument,no-self-use
    def _handle_subscriptions(self, data, ts):
        return

    # pylint: disable=no-self-use
    def _handle_error(self, *args):
        print(args)

    def _handle_quotes(self, data, ts):
        try:
            changes = data['changes']
        except KeyError:
            changes = data
        uid = None
        pair = data['product_id']
        bids = [(*item[1:], uid, ts) for item in changes if item[0] == 'buy']
        asks = [(*item[1:], uid, ts) for item in changes if item[0] == 'sell']
        self.q.put((pair, 'Quotes', (bids + asks)))

    def _handle_book(self, data, ts):
        pair = data['product_id']
        bids = [(*item, ts) for item in data['bids']]
        asks = [(*item, ts) for item in data['asks']]

        self.q.put((pair, 'Book', (bids, asks, ts)))
        bid_quotes = [(pair, price, size, 'bid', None, ts) for price, size, ts in bids]
        ask_quotes = [(pair, price, size, 'ask', None, ts) for price, size, ts in asks]
        self.q.put((pair, 'Quotes', bid_quotes + ask_quotes))

    def _handle_raw_book(self, data, ts):
        pass
