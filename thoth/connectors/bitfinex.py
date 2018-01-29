# Import Built-Ins
import logging
import json
import time
import hashlib
import hmac
from collections import OrderedDict

# Import Third-Party
import websocket

# Import Homebrew
from thoth.core.websocket import WebsocketConnector
# Init Logging Facilities
log = logging.getLogger(__name__)


class BitfinexConnector(WebsocketConnector):
    """Websocket Connection Thread

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    It handles all low-level system messages, such a reconnects, pausing of
    activity and continuing of activity.
    """
    def __init__(self, url, timeout=None, reconnect_interval=None, log_level=None,
                 q_maxsize=None):
        """Initialize a WebSocketConnection Instance.

        :param data_q: Queue(), connection to the Client Class
        :param args: args for Thread.__init__()
        :param url: websocket address, defaults to v2 websocket.
        :param timeout: timeout for connection; defaults to 10s
        :param reconnect_interval: interval at which to try reconnecting;
                                   defaults to 10s.
        :param log_level: logging level for the connection Logger. Defaults to
                          logging.INFO.
        """
        super(WebSocketConnection, self).__init__(url, timeout=timeout, log_level=log_level,
                                                  reconnect_interval=reconnect_interval,
                                                  q_maxsize=q_maxsize)

        # Dict to store all subscribe commands for reconnects
        self.channel_configs = OrderedDict()

    def send(self, api_key=None, secret=None, list_data=None, auth=False, **kwargs):
        """Sends the given Payload to the API via the websocket connection."""
        if auth:
            nonce = str(int(time.time() * 10000000))
            auth_string = 'AUTH' + nonce
            auth_sig = hmac.new(secret.encode(), auth_string.encode(),
                                hashlib.sha384).hexdigest()

            payload = {'event': 'auth', 'apiKey': api_key, 'authSig': auth_sig,
                       'authPayload': auth_string, 'authNonce': nonce}
        elif list_data:
            payload = list_data
        else:
            payload = kwargs
        self.log.debug("send(): Sending payload to API: %s", payload)
        try:
            super(WebSocketConnection, self).send(payload)
        except websocket.WebSocketConnectionClosedException:
            self.log.error("send(): Did not send out payload %s - client not connected. ", kwargs)

    def _pause(self):
        """Pauses the connection.

        :return:
        """
        self.log.debug("_pause(): Setting paused() Flag!")
        self.paused.set()

    def _unpause(self):
        """Unpauses the connection.

        Send a message up to client that he should re-subscribe to all
        channels.

        :return:
        """
        self.log.debug("_unpause(): Clearing paused() Flag!")
        self.paused.clear()
        self.log.debug("_unpause(): Re-subscribing softly..")
        self._resubscribe(soft=True)

    def pass_up(self, data, recv_at):
        # Handle data
        if isinstance(data, dict):
            # This is a system message
            self._system_handler(data, recv_at)
        else:
            # This is a list of data
            if data[1] == 'hb':
                self._heartbeat_handler()
            else:
                self._data_handler(data, recv_at)

    def _heartbeat_handler(self):
        """Handles heartbeat messages."""
        # Restart our timers since we received some data
        self.log.debug("_heartbeat_handler(): Received a heart beat "
                       "from connection!")
        self._start_timers()

    def _pong_handler(self):
        """Handle a pong response."""
        # We received a Pong response to our Ping!
        self.log.debug("_pong_handler(): Received a Pong message!")
        self.pong_received = True

    def _system_handler(self, data, ts):
        """Distributes system messages to the appropriate handler.

        System messages include everything that arrives as a dict,
        or a list containing a heartbeat.

        :param data:
        :param ts:
        :return:
        """
        self.log.debug("_system_handler(): Received a system message: %s", data)
        # Unpack the data
        event = data.pop('event')
        if event == 'pong':
            self.log.debug("_system_handler(): Distributing %s to _pong_handler..",
                      data)
            self._pong_handler()
        elif event == 'info':
            self.log.debug("_system_handler(): Distributing %s to _info_handler..",
                      data)
            self._info_handler(data)
        elif event == 'error':
            self.log.debug("_system_handler(): Distributing %s to _error_handler..",
                      data)
            self._error_handler(data)
        elif event in ('subscribed', 'unsubscribed', 'conf', 'auth', 'unauth'):
            self.log.debug("_system_handler(): Distributing %s to "
                           "_response_handler..", data)
            self._response_handler(data, ts)
        else:
            self.log.error("Unhandled event: %s, data: %s", event, data)

    def _response_handler(self, data, ts):
        """Handle responses to (un)subscribe and conf commands."""
        self.log.debug("_response_handler(): Passing %s to client..", data)
        self.pass_up(data, ts)

    def _info_handler(self, data):
        """Handle INFO messages from the API and issues relevant actions."""

        def raise_exception():
            """Log info code as error and raise a ValueError."""
            self.log.error("%s: %s", data['code'], info_message[data['code']])
            raise ValueError("%s: %s" % (data['code'], info_message[data['code']]))

        if 'code' not in data and 'version' in data:
            self.log.info('Initialized Client on API Version %s', data['version'])
            return

        codes = {'200000': raise_exception, '20051': self.reconnect, '20060': self._pause,
                 '20061': self._unpause}
        info_message = {'20000': 'Invalid User given! Please make sure the given ID is correct!',
                        '20051': 'Stop/Restart websocket server '
                                 '(please try to reconnect)',
                        '20060': 'Refreshing data from the trading engine; '
                                 'please pause any acivity.',
                        '20061': 'Done refreshing data from the trading engine.'
                                 ' Re-subscription advised.'}
        try:
            self.log.info(info_message[data['code']])
            codes[data['code']]()
        except KeyError as e:
            log.exception(e)
            raise

    def _error_handler(self, data):
        """Handle Error messages and log them accordingly."""
        errors = {10000: 'Unknown event',
                  10001: 'Unknown pair',
                  10300: 'Subscription Failed (generic)',
                  10301: 'Already Subscribed',
                  10302: 'Unknown channel',
                  10400: 'Subscription Failed (generic)',
                  10401: 'Not subscribed',
                  }
        try:
            self.log.error(errors[data['code']])
        except KeyError as e:
            # Unknown error code, log it and reconnect.
            log.exception(e)
            self.log.error("Received unknown error Code in message %s! "
                           "Reconnecting..", data)

    def _data_handler(self, data, ts):
        """Handle data messages by passing them up to the client."""
        self.pass_up(data, ts)

    def _resubscribe(self, soft=False):
        """Resubscribes to all channels found in self.channel_configs.

        :param soft: if True, unsubscribes first.
        :return: None
        """
        q_list = []
        while True:
            try:
                identifier, q = self.channel_configs.popitem(last=True if soft else False)
            except KeyError:
                break
            q_list.append((identifier, q.copy()))
            if identifier == 'auth':
                self.send(**q, auth=True)
                continue
            if soft:
                q['event'] = 'unsubscribe'
            self.send(**q)

        # Resubscribe for soft start.
        if soft:
            for identifier, q in reversed(q_list):
                self.channel_configs[identifier] = q
                self.send(**q)
        else:
            for identifier, q in q_list:
                self.channel_configs[identifier] = q
