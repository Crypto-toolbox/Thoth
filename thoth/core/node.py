"""Basic Node for a data cluster."""

# pylint: disable=no-name-in-module,unexpected-keyword-arg,too-many-branches

# Import Built-Ins
import logging
import queue

# Import Third-Party

# Import Home-grown
from hermes import Node

# Init Logging Facilities
log = logging.getLogger(__name__)


class DataNode(Node):
    """Provides a websocket interface and publisher.

    the topic tree is built as follows:
        <dtype>/<pair (if applicable)>/<node name>
    """

    # pylint: disable=too-many-instance-attributes

    def run(self):
        """Execute main loop.

        For data to be processed, it needs to have a certain format when it is returned from
        the internal :attr:`thoth.DataNode.receiver` object, which is as follows:
            (pair, dtype, data)

        This is necessary for the :class:`thoth.DataNode` to be able to call the proper
        publishing method as well as passing along the correct meta data
        (channel name, for example). If there more or less than 3 values in the tuple, the
        message will not be processed and instead is logged as an error.

        If the format is valid, data is published raw, as well as on the respective dtype channel,
        if available.
        """
        while self._running:
            try:
                msg = self.recv(block=False, timeout=None)
            except (TimeoutError, queue.Empty):
                continue
            if msg:
                try:
                    pair, dtype, data = msg
                except ValueError as e:
                    log.exception(e)
                    log.error(msg)
                    continue
                self.publish_raw(data)
                try:
                    if dtype == 'Book':
                        self.publish_book(pair, data)
                    elif dtype == 'RawBook':
                        self.publish_raw_book(pair, data)
                    elif dtype == 'TopLevel':
                        self.publish_top_level(pair, data)
                    elif dtype == 'Candle':
                        self.publish_candle(pair, data)
                    elif dtype == 'Trades':
                        self.publish_trades(data)
                    elif dtype == 'Quote':
                        self.publish_quote(data)
                    else:
                        pass
                except ValueError as e:
                    log.exception(e)
                    log.error("Could not unpack data: %r", data)
                    continue

    def publish_raw(self, data):
        """
        Publish the raw connection data.

            data = "data as received by self.recv()"

        :param data: Python data obj containing the formatted data
        :return: bool
        """
        return self.publish('raw', data)

    def publish_top_level(self, pair, data):
        """
        Publish level 1 order book data.

        data is a list or tuple with 3 elements, 2 quotes and a timestamp:
            data = (bid_price, bid_size, ts), (ask_price, ask_size, ts), timestamp

        :param pair:
        :param data:
        :return:
        """
        bid, ask, ts = data
        struct = TopLevel(pair, bid, ask, ts)
        return self.publish('TopLevel/%s' % pair, struct)

    def publish_book(self, pair, data):
        """
        Publish level 2 order book data.

        Data published on this channel is always a snapshot of the entire book.

        data is a list or tuple with 2 elements, each being a list/tuple of tuples each:
            data = [(price, size, ts), ..], [[(price, size, ts), ..]], timestamp

        :param pair: Currency pair this data relates to
        :param data: Python data obj containing the formatted data
        :return: bool
        """
        bids, asks, ts = data
        struct = Book(pair, bids, asks, ts=ts)
        return self.publish('Book/%s' % pair, struct)

    def publish_raw_book(self, pair, data):
        """
        Publish level 3 order book data.

        Data published on this channel is always a snapshot of the entire book.

        data is a list or tuple with 2 elements, each being a list/tuple of tuples each:
            data = [(price, size, ts, uid), ..], [[(price, size, ts, uid), ..]], timestamp

        :param pair: Currency pair this data relates to
        :param data: Python data obj containing the formatted data
        :return: bool
        """
        bids, asks, ts = data
        struct = RawBook(pair, bids, asks, ts)
        return self.publish('RawBook/%s' % pair, struct)

    def publish_quote(self, data):
        """
        Publish created, modified or cancelled/filled Quotes.

        Use this channel to maintain your own L3 Order book.

        It is important to note that by default, we do not differentiate between filled
        and deleted quotes.

        data is a tuple of:
            pair, price, size, side, uid, api_ts

        :param pair: Currency pair this data relates to
        :param data: Python data obj containing the formatted data
        :return:
        """
        pair, price, size, side, uid, api_ts = data
        struct = Quote(pair, price, size, side, uid, api_ts)
        return self.publish('Quote/%s' % pair, struct)

    def publish_candle(self, pair, data):
        """
        Publish candle (OHLC) data.

        data should be as follows:
            data = open, high, low, close, ts

        :param pair: Currency pair this data relates to
        :param data: Python data obj containing the formatted data
        :return: bool
        """
        open_price, high, low, close, ts = data
        struct = Candle(pair, open_price, high, low, close, ts)
        return self.publish('Candle/%s' % pair, struct)

    def publish_trades(self, data):
        """
        Publish trades for the given pair.

        data should be a list or tuple  of trades as tuples:
            data = [(pair, price, size, side, uid, misc, ts), ..]

        :param pair: Currency pair this data relates to
        :param data: hermes.structs.Trade instance
        :return: bool
        """
        trades = data
        struct = Trades(*trades)
        return self.publish('Trades', struct)
