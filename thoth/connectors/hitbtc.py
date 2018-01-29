"""HitBTC Connector which pre-formats incoming data to the CTS standard."""

import logging
import time
import json
from threading import Timer

from collections import defaultdict

import requests

from thoth.connectors.base import WebsocketConnector


log = logging.getLogger(__name__)

# pylint: disable=duplicate-code


class HitBTCConnector(WebsocketConnector):
    """Class to pre-process HitBTC data, before passing it up to a Node."""

    def __init__(self, **conn_ops):
        """Initialize a HitBTCConnector instance."""
        url = 'wss://api.hitbtc.com/api/2/ws'
        super(HitBTCConnector, self).__init__(url, **conn_ops)
        self.books = defaultdict(dict)
        self.channel_handlers = {'ticker': self._handle_ticker,
                                 'snapshotOrderbook': self._handle_book,
                                 'updateOrderbook': self._handle_book,
                                 'snapshotTrades': self._handle_trades,
                                 'updateTrades': self._handle_trades,
                                 'snapshotCandles': self._handle_candles,
                                 'updateCandles': self._handle_candles}
        self.requests = {}

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

    # pylint: disable=arguments-differ,unused-argument
    def pass_up(self, decoded_message, ts):
        """Handle and pass received data to the appropriate handlers."""
        # pylint: disable=broad-except
        if 'jsonrpc' in decoded_message:
            if 'id' in decoded_message and 'result' in decoded_message:
                self._handle_response(decoded_message)
            elif 'error' in decoded_message:
                self._handle_error(decoded_message)
            else:
                try:
                    method = decoded_message['method']
                    symbol = decoded_message['params'].pop('symbol')
                    params = decoded_message.pop('params')
                except Exception as e:
                    self.log.exception(e)
                    self.log.error(decoded_message)
                self.channel_handlers[method](method, symbol, params)
        return

    def _handle_response(self, decoded_msg):
        """Handle JSONRPC response objects."""
        try:
            result = decoded_msg['result']
            i_d = decoded_msg['id']
        except KeyError as e:
            self.log.exception(e)
            self.log.error("An expected Key was not found in %s", decoded_msg)
            raise
        try:
            request = self.requests.pop(i_d)
            state = 'was processed successfully' if result is True else 'failed'
            self.log.info("Request #%s (Payload %s) %s!", i_d, request, state)
        except KeyError as e:
            log.exception(e)
            log.error("Could not find Request relating to Response object %s", decoded_msg)
            raise

    @staticmethod
    def _handle_error(decoded_msg):
        """Handle Error messages."""
        log.error(decoded_msg)

    # pylint: disable=unused-argument
    def _handle_ticker(self, method, symbol, params):
        """Handle streamed ticker data."""
        bid_price, ask_price = params['bid'], params['ask']
        open_, high, low, last = params['open'], params['high'], params['low'], params['last']
        vol, quote_vol = params['volume'], params['volumeQuote']
        timestamp = params['timestamp']
        self.q.put((symbol, 'Ticker', (bid_price, ask_price, open_, high, low, last, vol, quote_vol,
                                       timestamp)))

    # pylint: disable=unused-argument
    def _handle_book(self, method, symbol, params):
        """Handle streamed order book data."""
        ts = time.time()
        bids, asks, sequence = params['bid'], params['ask'], params['sequence']
        bids = {bid['price']: (bid['price'], bid['size'], str(sequence)) for bid in bids}
        asks = {ask['price']: (ask['price'], ask['size'], str(sequence)) for ask in asks}
        if not self.books[symbol]:
            self.books[symbol]['bids'] = bids
            self.books[symbol]['asks'] = asks
        else:
            for price in bids:
                if bids[price][1] == '0.00':
                    self.books[symbol]['bids'].pop(price)
                else:
                    self.books[symbol]['bids'][price] = bids[price]
            for price in asks:
                if asks[price][1] == '0.00':
                    self.books[symbol]['asks'].pop(asks[price])
                else:
                    self.books[symbol]['asks'][price] = asks[price]

        prepped_bids = sorted([self.books[symbol]['bids'][bid]
                               for bid in self.books[symbol]['bids']],
                              key=lambda x: float(x[0]), reverse=True)
        prepped_asks = sorted([self.books[symbol]['asks'][ask]
                               for ask in self.books[symbol]['asks']],
                              key=lambda x: float(x[0]))

        self.q.put((symbol, 'Book', (prepped_bids, prepped_asks, ts)))
        self.q.put((symbol, 'TopLevel', (prepped_bids[0], prepped_asks[0], ts)))

    # pylint: disable=unused-argument
    def _handle_trades(self, method, symbol, params):
        """Handle streamed trades data."""
        trades = params['data']
        prepped_trades = []
        for trade in trades:
            ts = trade['timestamp']
            price, size = trade['price'], trade['quantity']
            side = 'ask' if trade['side'] == 'sell' else 'bid'
            uid = trade['id']
            prepped_trades.append((symbol, price, size, side, uid, None, ts))
        self.q.put((symbol, 'Trades', prepped_trades))

    # pylint: disable=unused-argument
    def _handle_candles(self, method, symbol, params):
        """Handle streamed candle data."""
        period, candles = params['period'], params['data']
        for candle in candles:
            ts = candle['timestamp']
            open_, close, low, high = candle['open'], candle['close'], candle['min'], candle['max']
            self.q.put((symbol, 'Candle-%s' % period, (open_, high, low, close, ts)))

    def subscribe(self):
        """Subscribe to all available channels."""
        channels = ['subscribeticker', 'subscribeOrderbook', 'subscribeTrades', 'subscribeCandles']
        response = requests.get('https://api.hitbtc.com/api/2/public/symbol').json()
        pairs = [d['id'] for d in response]
        for pair in pairs:
            for channel in channels:
                if channel == 'subscribeCandles':
                    self.send(channel, symbol=pair, period='M1')
                else:
                    self.send(channel, symbol=pair)
                time.sleep(.5)

    # pylint: disable=arguments-differ
    def send(self, method, **params):
        """
        Send the given Payload to the API via the websocket connection.

        :param kwargs: payload parameters as key=value pairs
        """
        payload = {'method': method, 'params': params, 'id': int(10000 * time.time())}
        self.requests[payload['id']] = payload
        self.log.debug("Sending: %s", payload)
        self.conn.send(json.dumps(payload))

    def send_ping(self):
        """Override ping command since HitBTC does not support this."""
        return

    def _check_pong(self):
        """Override pong check since HitBTC does not support this."""
        return
