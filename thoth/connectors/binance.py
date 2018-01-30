"""Poloniex Connector which pre-formats incoming data to the CTS standard."""

import logging
import json
import time

from thoth.core.websocket import WebSocketConnector


log = logging.getLogger(__name__)


class BinanceConnector(WebSocketConnector):
    """Class to pre-process HitBTC data, before passing it up to a Node."""
    channels = ['%s@trade', '%s@aggTrade', '%s@kline_1m', '%s@ticker', '%s@depth']

    def __init__(self, pairs, **conn_ops):
        """Initialize a PoloniexConnector instance."""
        streams = []
        for pair in pairs:
            for chan in self.channels:
                streams.append(chan % pair)
        url = 'wss://stream.binance.com:9443/stream?streams=' + '/'.join(streams)
        super(BinanceConnector, self).__init__(url, **conn_ops)
        self.pairs = pairs

    def _on_message(self, ws, data):
        message = json.loads(data)
        mtype = message['e']

        if mtype in ('aggTrade',):
            topic = 'aggTrade_' + data['s']
        elif mtype in ('trade',):
            topic = 'trades_' + data['s']
        elif mtype in ('kline',):
            topic = 'candle_' + data['s'] + '/' + data['k']['i']
        elif mtype == '24h_ticker':
            topic = 'ticker_' + data['s']
        elif mtype == 'depthUpdate':
            topic = 'diff_book_' + data['s']
        else:
            log.error(message)
            return
        super(BinanceConnector, self)._on_message(ws, (topic, data, time.time()))


