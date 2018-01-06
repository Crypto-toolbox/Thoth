import unittest
import threading

from hermes.proxy import ClusterInterfaceProxy
from cts_core import Receiver
from hermes.structs import Book, RawBook, TopLevel, Candle, Trades, Order

XPUB_ADDR = 'tcp://127.0.0.1:%s'
XSUB_ADDR = 'tcp://127.0.0.1:%s'
DEBUG_ADDR = 'tcp://127.0.0.1:%s'


class GenericDataFormatTestCase(unittest.TestCase):
    """Base class for data format tests, offering standardized test methods."""
    def __init__(self, xsub_port, xpub_port, debug_port, *args, **kwargs):
        """Initialize instance."""
        super(GenericDataFormatTestCase, self).__init__(*args, **kwargs)
        self.proxy = ClusterInterfaceProxy(XSUB_ADDR % xsub_port, XPUB_ADDR % xpub_port,
                                           DEBUG_ADDR % debug_port)
        self.proxy_thread = threading.Thread(target=self.proxy.run, daemon=True)
        self.proxy_thread.start()
        self.DEBUG_ADDR = DEBUG_ADDR % debug_port

    def get_cts_msg(self, data_node, topics):
        receiver = Receiver(self.DEBUG_ADDR, 'TestProxy %s' % topics, topics)
        receiver.start()
        t = threading.Thread(target=data_node.run, daemon=True)
        t.start()
        cts_msg = None
        while cts_msg is None:
            cts_msg = receiver.recv()
        receiver.stop(1)
        return cts_msg

    def check_topic_and_origin(self, cts_message, expected_topic, expected_origin):
        """Assert that both topic and origin are correct.

        :param cts_message: CTSMsg instance
        :param expected_topic: str
        :param expected_origin: str
        :return:
        """
        self.assertEqual(cts_message.origin, expected_origin)
        self.assertEqual(cts_message.topic, expected_topic)

    def check_orderbook_channel_format(self, data_node, topics):
        """Test the format of order book data.

        Expected output:
            hermes.structs.Book instance
                .bids: (('price', 'size', 'ts'), ...)
                .asks: (('price', 'size', 'ts'), ...)

        :param data_node: Node Object to test
        :param topics: bytes object, defines topic string
        :return: CTSMsg
        """
        cts_msg = self.get_cts_msg(data_node, topics)
        self.assertIsInstance(cts_msg.data, Book)

        self.assertIsInstance(cts_msg.data.bids, (list, tuple))
        for bid in cts_msg.data.bids:
            self.assertIsInstance(bid, (list, tuple), msg="Quote %s in bids of %s does not have "
                                                          "expected dtype!" % (bid, cts_msg.data))
            self.assertEqual(len(bid), 3, msg="Quote %s in bids of %s does not have "
                                              "expected length!" % (bid, cts_msg.data))
            for x in bid:
                self.assertIsInstance(x, str, msg="Element in bid %s  bids of %s does not have "
                                                  "expected dtype!" % (bid, cts_msg.data))

        self.assertIsInstance(cts_msg.data.asks, (list, tuple))
        for ask in cts_msg.data.asks:
            self.assertIsInstance(ask, (list, tuple), msg="Quote %s in asks of %s does not have "
                                                          "expected dtype!" % (ask, cts_msg.data))
            self.assertEqual(len(ask), 3, msg="Quote %s in asks of %s does not have "
                                              "expected length!" % (ask, cts_msg.data))
            for x in ask:
                self.assertIsInstance(x, str, msg="Element in ask %s  bids of %s does not have "
                                                  "expected dtype!" % (x, cts_msg.data))
        return cts_msg

    def check_orderbook_raw_channel_format(self, data_node, topics):
        """Test the format of raw order book data.

        Expected output:
            hermes.structs.Book instance
                .bids: (('price', 'size', 'ts', 'tid'), ...)
                .asks: (('price', 'size', 'ts', 'tid'), ...)

        :param data_node: Node Object to test
        :param topics: bytes object, defines topic string
        :return: CTSMsg
        """
        cts_msg = self.get_cts_msg(data_node, topics)
        self.assertIsInstance(cts_msg.data, RawBook)
        self.assertIsInstance(cts_msg.data.bids, (list, tuple))
        for bid in cts_msg.data.bids:
            self.assertIsInstance(bid, (list, tuple), msg="Quote %s in bids of %s does not have "
                                                          "expected dtype!" % (bid, cts_msg.data))
            self.assertEqual(len(bid), 4, msg="Quote %s in bids of %s does not have "
                                              "expected length!" % (bid, cts_msg.data))
            for x in bid:
                self.assertIsInstance(x, str, msg="Element in bid %s  bids of %s does not have "
                                                  "expected dtype!" % (bid, cts_msg.data))

        self.assertIsInstance(cts_msg.data.asks, (list, tuple))
        for ask in cts_msg.data.asks:
            self.assertIsInstance(ask, (list, tuple), msg="Quote %s in asks of %s does not have "
                                                          "expected dtype!" % (ask, cts_msg.data))
            self.assertEqual(len(ask), 4, msg="Quote %s in asks of %s does not have "
                                              "expected length!" % (ask, cts_msg.data))
            for x in ask:
                self.assertIsInstance(x, str, msg="Element in ask %s  bids of %s does not have "
                                                  "expected dtype!" % (x, cts_msg.data))
        return cts_msg

    def check_ticker_channel_format(self, data_node, topics):
        """Test the format of ticker book data.

        Expected output:
            hermes.structs.TopLevel instance
                .bid: ('price', 'size', 'ts')
                .ask: ('price', 'size', 'ts')

        :param data_node: Node Object to test
        :param topics: bytes object, defines topic string
        :return: CTSMsg
        """
        cts_msg = self.get_cts_msg(data_node, topics)
        self.assertIsInstance(cts_msg.data, TopLevel)
        bid, ask, ts = cts_msg.data
        self.assertIsInstance(cts_msg.data.bid, (list, tuple), msg="Bid does not have valid dtype!")
        self.assertEqual(len(cts_msg.data.bid), 3, msg="Bid in %s does not have expected length!" %
                                                       cts_msg.data)
        self.assertIsInstance(cts_msg.data.ask, (list, tuple), msg="Ask does not have valid dtype!")
        self.assertEqual(len(cts_msg.data.ask), 3, msg="Ask in %s does not have expected length!" %
                                                       cts_msg.data)
        self.assertIsInstance(cts_msg.data.ts, str, msg="Timestamp does not have valid dtype!")
        for item in bid:
            self.assertIsInstance(item, str, msg="Item in Bid %s does not have "
                                                 "expected dtype!" % bid)
        for item in ask:
            self.assertIsInstance(item, str, msg="Item in Ask %s does not have "
                                                 "expected dtype!" % ask)
        return cts_msg

    def check_candle_channel_format(self, data_node, topics):
        """Test the format of candle book data.

        Expected output:
            hermes.structs.Candle instance
                .open: 'price'
                .high: 'price'
                .low: 'price'
                .close: 'price'

        :param data_node: Node Object to test
        :param topics: bytes object, defines topic string
        :return: CTSMsg
        """
        cts_msg = self.get_cts_msg(data_node, topics)
        self.assertIsInstance(cts_msg.data, Candle)

        self.assertIsInstance(cts_msg.data.open, str)
        try:
            float(cts_msg.data.open)
        except ValueError:
            self.fail("cts_msg.data.open cannot be converted to 'float' type! struct: %r" %
                      cts_msg.data)

        self.assertIsInstance(cts_msg.data.high, str)
        try:
            float(cts_msg.data.high)
        except ValueError:
            self.fail("cts_msg.data.high cannot be converted to 'float' type! struct: %r" %
                      cts_msg.data)

        self.assertIsInstance(cts_msg.data.low, str)
        try:
            float(cts_msg.data.low)
        except ValueError:
            self.fail("cts_msg.data.low cannot be converted to 'float' type! struct: %r" %
                      cts_msg.data)

        self.assertIsInstance(cts_msg.data.close, str)
        try:
            float(cts_msg.data.close)
        except ValueError:
            self.fail("cts_msg.data.close cannot be converted to 'float' type! struct: %r" %
                      cts_msg.data)

        return cts_msg
