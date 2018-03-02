"""Pusher Connector Base class.

Uses zeromq to pass data upward, using PUSH/PULL.

Passes data without touching it.
"""

# pylint: disable=too-many-arguments

# Import Built-Ins
import logging
import time

# Import Third-party
from pysher import Pusher
import zmq

# Import home-grown

# Init Logging Facilities
log = logging.getLogger(__name__)


class PusherConnector(Pusher):
    """Websocket Connector for Pusher-based APIs.


    :meth:``PusherConnector._connect_channels`` is called upon opening a connection to Pusher.
    This method, by default, calls :meth:``PusherConnector._base_callback`` for each pair in
    :attr:``PusherConnector.pairs``. It may be replaced by ``_base_callback`` directly if you
    do not have to handle several pairs, or want to handle all pairs in a single method instead.
    """

    def __init__(self, pairs, *pusher_args, ctx=None, zmq_addr=None, **pusher_kwargs):
        """Initialize Connector."""
        super(PusherConnector, self).__init__(*pusher_args, **pusher_kwargs)
        self.pairs = pairs
        self.ctx = ctx or zmq.Context()
        self.q = self.ctx.socket(zmq.PUSH)
        self.zmq_addr = zmq_addr
        self.q.bind(self.zmq_addr)
        self.connection.bind('pusher:connection_established', self._connect_channels)

    def push(self, topic, data, recv_at):
        """Push data upwards."""
        payload = [topic.encode('UTF-8'), data.encode('UTF-8'), str(recv_at).encode('UTF-8')]
        self.q.send_multipart(payload)

    def _connect_channels(self, data):
        """Connect all available channels to this connector."""
        for pair in self.pairs:
            self._base_callback(data, pair)

    def stop(self):
        """Stop the connector."""
        self.disconnect()
        self.q.close()

    def start(self):
        """Start the connector."""
        self.connect()

    def recv(self, block=True, timeout=None):
        """Wrap for self.q.get().

        :param block: Whether or not to make the call to this method block
        :param timeout: Value in seconds which determines a timeout for get()
        :return:
        """
        return self.q.get(block, timeout)

    # pylint: disable=unused-argument
    def _base_callback(self, data, pair):
        """Put data on respective queue."""
        def callback_a(data):
            """Put data on q with correct channel name."""
            print(data)
            self.push('raw', data, time.time())

        def callback_b(data):
            """Put data on q with correct channel name."""
            print(data)
            self.push('raw', data, time.time())

        channel1 = self.subscribe('Channel_A')
        channel1.bind('EVENT_NAME', callback_a)
        channel2 = self.subscribe('Channel_B')
        channel2.bind('EVENT_NAME', callback_b)