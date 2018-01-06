"""Poloniex Connector which pre-formats incoming data to the CTS standard."""

import logging
from threading import Timer

import requests

from thoth.connectors.base import WebSocketConnectorThread


log = logging.getLogger(__name__)

# pylint: disable=duplicate-code


class PoloniexConnector(WebSocketConnectorThread):
    """Class to pre-process HitBTC data, before passing it up to a Node."""

    def __init__(self, **conn_ops):
        """Initialize a PoloniexConnector instance."""
        url = 'wss://api2.poloniex.com/'
        super(PoloniexConnector, self).__init__(url, **conn_ops)
        pair_dict = requests.get('https://poloniex.com/public?command=returnTicker').json()
        self.pairs = {pair_dict[k]['id']: k for k in pair_dict}

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

    # pylint: disable=too-many-locals,broad-except,too-many-branches,too-many-statements
    def pass_up(self, data, recv_at):
        """Process data and put it on the internal queue."""
        recv_at = str(recv_at)
        try:
            channel, sequence, payload = data
        except ValueError as e:
            if len(data) == 2:
                channel, ack = data
                log.info("Subscription to channel %s: %s", channel, True if ack else False)
            elif len(data) == 1:
                print("Heartbeat:", data)
            else:
                print("ERROR:", data)
                log.exception(e)
            return
        if channel in (1002, '1002'):
            #  ticker data
            pair, *ticker_data = payload
            self.q.put((pair, 'Ticker', ticker_data))
        else:
            #  book data
            try:
                pair = self.pairs[channel]
            except Exception as e:
                log.exception(e)
                log.error(data)
                return
            quotes = []
            trades = []
            snapshots = []
            for package in payload:
                dtype, *package = package

                if dtype == 't':
                    # trade data - store sequence in misc field, set uid to None since we do not
                    # have one.
                    uid, side, price, size, ts = package
                    packed_data = (pair, price, size, 'ask' if side else 'bid', uid,
                                   sequence, recv_at)
                    trades.append(packed_data)
                elif dtype == 'o':
                    # order (post, modify, delete) - these need to be collected before posted
                    side, price, size = package
                    packed_data = pair, price, size, 'ask' if side else 'bid', sequence, recv_at
                    quotes.append(packed_data)
                    continue
                elif dtype == 'i':
                    #  initial snapshot of the order book
                    asks, bids = package[0]['orderBook']
                    for price, size, ts in asks:
                        quotes.append((pair, price, size, 'ask', None, ts))
                    for price, size, ts in bids:
                        quotes.append((pair, price, size, 'bid', None, ts))
                    snapshots.append((bids, asks, recv_at))
                else:
                    raise ValueError("Unhandled package type in package: %s" % package)
            if quotes:
                dtype = 'Quotes'
                self.q.put((pair, dtype, quotes))
            if trades:
                dtype = 'Trades'
                self.q.put((pair, dtype, trades))
            if snapshots:
                dtype = 'Book'
                for snapshot in snapshots:
                    self.q.put((pair, dtype, snapshot))
