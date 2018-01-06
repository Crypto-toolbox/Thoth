"""The Rock Trading Ltd. Websocket connector."""

import time
import logging
import json

from thoth.connectors.base import PusherConnector

log = logging.getLogger(__name__)


class RockTradingConnector(PusherConnector):
    """Websocket Connector for the Pusher-based Bitstamp API."""

    def __init__(self, *args, **kwargs):
        """Initialize Connector."""
        pairs = 'BTCEUR,BTCUSD,LTCEUR,LTCBTC,BTCXRP,EURXRP,USDXRP,PPCEUR,PPCBTC,ETHEUR,ETHBTC,' \
                'ZECBTC,ZECEUR,BCHBTC,EURNEUR,EURNBTC'.split(',')
        super(RockTradingConnector, self).__init__(pairs, *args, **kwargs)

    # pylint: disable=unused-argument
    def _base_callback(self, *, pair, channels, data):
        """Put data on respective queue."""
        def new_offer(data):
            """Put data on q with correct channel name."""
            pass

        def last_trade(data):
            """Put data on q with correct channel name."""
            decoded_data = json.loads(data)
            price, size = str(decoded_data['value']), str(decoded_data['quantity'])
            side = 'ask' if decoded_data['side'] == 'sell' else 'bid'
            uid = None
            ts = decoded_data['time']
            trade_data = pair, price, size, side, uid, None, ts
            msg = pair, "Trades", [trade_data]
            self.q.put(msg)

        def last_volume(data):
            """Handle last volume data."""
            pass

        def book(data):
            """Put data on q with correct channel name."""
            unpacked_data = json.loads(data)
            ts = str(time.time())
            formatted_data = [[(str(d['price']), str(d['amount']), ts)
                               for d in unpacked_data['bids']],
                              [(str(d['price']), str(d['amount']), ts)
                               for d in unpacked_data['asks']], ts]

            msg = pair, "Book", formatted_data
            quotes = [[pair, price, size, 'bid', None, ts] for price, size, ts in
                      formatted_data[0]]
            quotes += [[pair, price, size, 'ask', None, ts] for price, size, ts in
                       formatted_data[1]]
            self.q.put(msg)
            self.q.put((pair, 'Quotes', quotes))

        def diff_book(data):
            """Handle diff book data."""
            pass

        channel1 = self.subscribe(pair)
        channel1.bind('new_offer', new_offer)
        channel1.bind('orderbook', book)
        channel1.bind('orderbook_diff', diff_book)
        channel1.bind('last_trade', last_trade)
        channel1.bind('last_volume', last_volume)

    def _pair_callback(self, data, pair):
        """Cal relevant function for given pair."""
        self._base_callback(pair=pair, channels=None, data=data)
