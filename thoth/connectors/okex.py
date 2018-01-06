"""OKEx Connector which pre-formats incoming data to the CTS standard."""

import logging
from threading import Timer


from thoth.connectors.base import WebSocketConnectorThread


log = logging.getLogger(__name__)

# pylint: disable=duplicate-code


class OKExConnector(WebSocketConnectorThread):
    """Class to pre-process HitBTC data, before passing it up to a Node."""

    def __init__(self, **conn_ops):
        """Initialize a PoloniexConnector instance."""
        url = 'wss://real.okex.com:10441/websocket '
        super(OKExConnector, self).__init__(url, **conn_ops)

    def _start_timers(self):
        """Reset and start timers for API connection."""
        self._stop_timers()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def _stop_timers(self):
        """Stop connection timer."""
        if self.connection_timer:
            self.connection_timer.cancel()

    # pylint: disable=unused-variable
    def _on_open(self, ws):
        """Subscribe upon connecting."""
        super(OKExConnector, self)._on_open(ws)
        pairs = 'ltc_btc eth_btc etc_btc bch_btc btc_usdt eth_usdt ltc_usdt etc_usdt bch_usdt ' \
                'etc_eth bt1_btc bt2_btc btg_btc qtum_btc hsr_btc neo_btc gas_btc qtum_usdt ' \
                'hsr_usdt neo_usdt gas_usdt iota_btc'.split(' ')
        for pair in ('eth_btc', 'iota_btc'):
            payload = {'event': 'addChannel', 'channel': 'ok_sub_spot_%s_depth_20' % pair}
            self.send(payload)

    def pass_up(self, data, recv_at):
        """Process the data and put it on the internal queue."""
        try:
            data = data[0]
            channel = data['channel']
            bids, asks = data['data']['bids'], data['data']['asks']
            bids = [[str(price), str(size), str(recv_at)] for price, size in bids]
            asks = [[str(price), str(size), str(recv_at)] for price, size in asks]
            pair, *_ = channel[12:].rsplit('_', maxsplit=2)
            self.q.put((pair, 'Book', (bids, asks, recv_at)))
            quotes = [[pair, p, s, 'ask', None, ts] for p, s, ts in bids]
            quotes += [[pair, p, s, 'ask', None, ts] for p, s, ts in asks]
            self.q.put((pair, 'Quotes', quotes))
        except (IndexError, KeyError):
            print(data)
