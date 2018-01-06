"""Gemini Websocket connector."""

import time
import logging
from threading import Timer
from thoth.connectors.base import WebSocketConnectorThread

log = logging.getLogger(__name__)


class GeminiConnector(WebSocketConnectorThread):
    """Gemini Websocket Connector."""

    def __init__(self, pair, **conn_ops):
        """Initialize GeminiConnector."""
        url = 'wss://api.gemini.com/v1/marketdata/' + pair
        self.pair = pair
        self.last_seq = None
        super(GeminiConnector, self).__init__(url, **conn_ops)

    def _start_timers(self):
        """Reset and start timers for API connection."""
        self._stop_timers()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def send_ping(self):
        """Override the send_ping method."""
        return

    def _stop_timers(self):
        """Stop connection timer."""
        if self.connection_timer:
            self.connection_timer.cancel()

    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    def pass_up(self, data, recv_at):
        """Preformat gemini Market data and pass it on to data node.

        :param data: raw websocket message
        :param recv_at:
        :return:
        """
        try:
            event = data['type']
        except KeyError:
            if 'result' in data:
                event = data['result']
                error_code = data['reason']
                error_msg = data['msg']
                self.log.error("_handle_market_events(): %s - %s", error_code,
                               error_msg)
                return event, time.time(), data
            raise

        sock_seq = data['socket_sequence']
        if self.last_seq and sock_seq != self.last_seq + 1:
            log.error("Socket sequence out of order! Last: %s, Current: %s",
                      self.last_seq, sock_seq)
            self.reconnect()
            return
        else:
            self.last_seq = sock_seq

        if event == 'heartbeat':
            return
        else:
            snapshot = {'bids': [], 'asks': []}
            changes = []
            trades = []
            try:
                api_ts = data['timestampms']
            except KeyError:
                api_ts = str(round(time.time()))

            seq = data['socket_sequence']
            # event_id = data['eventId']
            # update message
            for event in data['events']:
                if event['type'] in ('auction_open', 'auction_indicative', 'auction_result'):
                    continue
                else:
                    price = event['price']
                    if event['type'] == 'change':
                        size = event['remaining']
                        side = event['side']
                        if event['reason'] == 'initial':
                            snapshot[event['side'] + 's'].append((price, size, api_ts))
                        else:
                            change = self.pair, price, size, side, None, api_ts
                            changes.append(change)

                    elif event['type'] == 'trade':
                        # trade
                        size = event['amount']
                        side = event['makerSide']
                        trades.append((self.pair, price, size, side, event['tid'], seq, api_ts))

            if snapshot['bids'] or snapshot['asks']:
                self.q.put((self.pair, 'Book', (snapshot['bids'], snapshot['asks'], api_ts)))
                bid_quotes = [(self.pair, price, size, 'bid', None, ts) for price, size, ts in
                              snapshot['bids']]
                ask_quotes = [(self.pair, price, size, 'ask', None, ts) for price, size, ts in
                              snapshot['asks']]
                changes += bid_quotes + ask_quotes

            if changes:
                self.q.put((self.pair, 'Quotes', changes))

            if trades:
                self.q.put((self.pair, 'Trades', trades))
