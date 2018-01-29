"""Quoinex Websocket connector."""

import logging
import requests

from thoth.core.pusher import PusherConnector


log = logging.getLogger(__name__)


class QuoinexConnector(PusherConnector):
    """Quoinex Websocket Connector."""

    def __init__(self, *args, **kwargs):
        """Initialize the instance."""
        super(QuoinexConnector, self).__init__(*args, **kwargs)
        response_data = requests.get('https://api.quoine.com/products').json()
        self.pairs = [d['currency_pair_code'] for d in response_data]

    # pylint: disable=unused-argument,arguments-differ
    def _base_callback(self):
        """Put data on respective queue."""
        def ticker(data):
            """Put data on q with correct channel name."""
            print(data)

        def book(data):
            """Put data on q with correct channel name."""
            print(data)

        def orders(data):
            """Put data on q with correct channel name."""
            print(data)

        def executions(data):
            """Put data on q with correct channel name."""
            print(data)

        channel1 = self.subscribe('')
        channel1.bind('trade', ticker)
        channel2 = self.subscribe('')
        channel2.bind('data', book)
        channel3 = self.subscribe('')
        channel3.bind('data', orders)
        channel4 = self.subscribe('')
        channel4.bind('order_deleted', executions)

    def _pair_callback(self, data, pair=None):
        """Cal relevant function for given pair."""
        self._base_callback()
