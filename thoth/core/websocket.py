"""Websocket Connector Base class."""

# pylint: disable=too-many-arguments

# Import Built-Ins
import logging
from queue import Queue
from threading import Thread, Timer

import json
import time
import ssl

# Import Third-Party
import websocket

# Import home-grown

# Init Logging Facilities
log = logging.getLogger(__name__)


class WebSocketConnector(Thread):
    """Websocket Connection Thread.

    Inspired heavily by ekulyk's PythonPusherClient Connection Class
    https://github.com/ekulyk/PythonPusherClient/blob/master/pusherclient/connection.py

    Data received is available by calling WebSocketConnection.recv()
    """

    # pylint: disable=too-many-instance-attributes, too-many-arguments,unused-argument

    def __init__(self, url, timeout=None, q_maxsize=None, reconnect_interval=None, log_level=None):
        """Initialize a WebSocketConnector Instance.

        :param url: websocket address, defaults to v2 websocket.
        :param timeout: timeout for connection; defaults to 10s
        :param reconnect_interval: interval at which to try reconnecting;
                                   defaults to 10s.
        :param log_level: logging level for the connection Logger. Defaults to
                          logging.INFO.
        :param args: args for Thread.__init__()
        :param kwargs: kwargs for Thread.__ini__()
        """
        # Queue used to pass data up to Node
        self.q = Queue(maxsize=-1 if not q_maxsize else q_maxsize)

        # Connection Settings
        self.url = url
        self.conn = None

        # Connection Handling Attributes
        self._is_connected = False
        self.disconnect_called = False
        self.reconnect_required = False
        self.reconnect_interval = reconnect_interval if reconnect_interval else 10
        self.paused = False

        # Setup Timer attributes
        # Tracks API Connection & Responses
        self.ping_timer = None
        self.ping_interval = 120

        # Set up history of sent commands for re-subscription
        self.history = []

        # Tracks Websocket Connection
        self.connection_timer = None
        self.connection_timeout = timeout if timeout else 10

        # Tracks responses from send_ping()
        self.pong_timer = None
        self.pong_received = False
        self.pong_timeout = 30

        self.log = logging.getLogger(self.__module__)
        self.log.setLevel(level=log_level if log_level else logging.INFO)
        if log_level == logging.DEBUG:
            websocket.enableTrace(True)

        formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s\t%(message)s')
        file_handler = logging.FileHandler(filename='wss.log', mode='w+')
        file_handler.setLevel(level=log_level if log_level else logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.log.addHandler(file_handler)

        super(WebSocketConnector, self).__init__()
        self.daemon = True

    def stop(self, timeout=None):
        """Wrap around disconnect().

         Supports stopping this thread via the hermes.Node._stop_facilities() method.

        :return:
        """
        self.disconnect()
        self.join(self, timeout)

    def disconnect(self):
        """Disconnect from the websocket connection and joins the Thread."""
        self.reconnect_required = False
        self.disconnect_called = True
        self._is_connected = False
        if self.conn:
            self.conn.close()

    def reconnect(self):
        """Issue a reconnection by setting the reconnect_required event."""
        # Reconnect attempt at self.reconnect_interval
        self.reconnect_required = True
        self._is_connected = False
        if self.conn:
            self.conn.close()

    def _connect(self):
        """Create a websocket connection.

        Automatically reconnects connection if it was severed unintentionally.
        """
        self.conn = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}
        self.conn.run_forever(sslopt=sslopt_ca_certs)

        while self.reconnect_required:
            if not self.disconnect_called:
                self.log.info("Attempting to connect again in %s seconds.", self.reconnect_interval)
                time.sleep(self.reconnect_interval)

                # We need to set this flag since closing the socket will
                # set it to False
                self.conn.keep_running = True
                self.conn.run_forever(sslopt=sslopt_ca_certs)

    def run(self):
        """Run the main method of thread."""
        self._connect()

    def _on_message(self, ws, message):
        """Handle and pass received data to the appropriate handlers.

        Resets timers for time-out countdown and logs exceptions during parsing.

        All messages are time-stamped

        :param ws: Websocket obj
        :param message: received data as bytes
        :return:
        """
        self._stop_timers()

        raw, received_at = message, time.time()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            # Something wrong with this data, log and discard
            self.log.exception("Exception %s for data %s; Discarding..", e, raw)
            return

        try:
            self.pass_up(data, received_at)
        except Exception as e:
            log.exception(e)
            log.error(data)
            raise

        # We've received data, reset timers
        self._start_timers()

    def _on_close(self, ws, *args):
        """Log the close and stop the time-out countdown.

        Execute when the connection is closed.

        :param ws: Websocket obj
        :param *args: additional arguments
        """
        self.log.info("Connection closed")
        self._is_connected = False
        self._stop_timers()

    def _on_open(self, ws):
        """Log connection status, set Events for _connect(), start timers and send a test ping.

        Execute on opening a new connection.

        If the connection was previously severed unintentionally, it re-subscribes
        to the channels by executing the commands found in self.history, in
        chronological order.

        :param ws: Webscoket obj
        """
        self.log.info("Connection opened")
        self._is_connected = True
        self.send_ping()
        self._start_timers()
        if self.reconnect_required:
            self.log.info("Reconnection successful, re-subscribing to"
                          "channels..")
            hist = self.history
            self.history = []
            for cmd in hist:
                self.send(cmd)

    def _on_error(self, ws, error):
        """Log the error, reset the self._is_connected flag and issue a reconnect.

        Callback executed on connection errors.

        Issued by setting self.reconnect_required.

        :param ws: Websocket obj
        :param error: Error message
        """
        self.log.info("Connection Error - %s", error)
        self._is_connected = False
        self.reconnect_required = True

    def _stop_timers(self):
        """Stop ping, pong and connection timers."""
        if self.ping_timer:
            self.ping_timer.cancel()

        if self.connection_timer:
            self.connection_timer.cancel()

        if self.pong_timer:
            self.pong_timer.cancel()

    def _start_timers(self):
        """Reset and start timers for API data and connection."""
        self._stop_timers()

        # Sends a ping at ping_interval to see if API still responding
        self.ping_timer = Timer(self.ping_interval, self.send_ping)
        self.ping_timer.start()

        # Automatically reconnect if we didnt receive data
        self.connection_timer = Timer(self.connection_timeout,
                                      self._connection_timed_out)
        self.connection_timer.start()

    def send_ping(self):
        """Send a ping message to the API and starts pong timers."""
        self.conn.send(json.dumps({'event': 'ping'}))
        self.pong_timer = Timer(self.pong_timeout, self._check_pong)
        self.pong_timer.start()

    def _check_pong(self):
        """Check if a Pong message was received."""
        self.pong_timer.cancel()
        if self.pong_received:
            self.pong_received = False
        else:
            # reconnect
            self.reconnect()

    def send(self, data):
        """Send the given Payload to the API via the websocket connection.

        Furthermore adds the sent payload to self.history.

        :param data: data to be sent
        :return:
        """
        if self._is_connected:
            payload = json.dumps(data)
            self.history.append(data)
            self.conn.send(payload)
        else:
            log.error("Cannot send payload! Connection not established!")

    def pass_up(self, data, recv_at):
        """Pass data up to the client via the internal Queue().

        :param data: data to be passed up
        :param recv_at: float, time of reception
        :return:
        """
        self.q.put(data, recv_at)

    def recv(self, block=True, timeout=None):
        """Wrap for self.q.get().

        :param block: Whether or not to make the call to this method block
        :param timeout: Value in seconds which determines a timeout for get()
        :return:
        """
        return self.q.get(block, timeout)

    def _connection_timed_out(self):
        """Issue a reconnection.

        :return:
        """
        self.reconnect()