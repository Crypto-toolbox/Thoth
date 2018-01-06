"""CEX.io Websocket connector."""

import time
import logging
import hmac
import hashlib
from threading import Timer
from thoth.connectors.base import WebSocketConnectorThread

log = logging.getLogger(__name__)


class CEXIOConnector(WebSocketConnectorThread):
    """CEX.io Websocket Connector."""

    def __init__(self, key, secret, **conn_ops):
        """Initialize CEXIOConnector."""
        url = 'wss://ws.cex.io/ws/'
        self.key, self.secret = key, secret
        self.last_seq = None
        super(CEXIOConnector, self).__init__(url, **conn_ops)

    def _start_timers(self):
        """Reset and start timers for API connection."""
        self._stop_timers()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    @staticmethod
    def nonce():
        """Generate a Nonce value as string."""
        return str(int(round(time.time())))

    def send_ping(self):
        """Send a ping to the server."""
        self.send({'e': 'ticker', 'data': ['BTC', 'USD'], 'oid': self.nonce()})

    def send_pong(self):
        """Send a pong response to the server."""
        self.send({'e': 'pong'})

    def _stop_timers(self):
        """Stop connection timer."""
        if self.connection_timer:
            self.connection_timer.cancel()

    def authenticate(self):
        """Authenticate with CEXio."""
        ts = self.nonce()
        raw_sig = ts + self.key
        sig = hmac.new(self.secret.encode('UTF-8'), raw_sig.encode('UTF-8'),
                       hashlib.sha256).hexdigest()
        payload = {'e': 'auth', 'auth': {'key': self.key, 'signature': sig, 'timestamp': ts}}
        self.send(payload)

    def pass_up(self, data, recv_at):
        """Preformat gemini Market data and pass it on to data node.

        :param data: raw websocket message
        :param recv_at:
        :return:
        """
        print(data, recv_at)
