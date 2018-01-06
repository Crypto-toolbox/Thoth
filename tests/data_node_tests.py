import unittest
from unittest import mock

import zmq

from utils import mocked_publisher, mocked_wss

from hermes.structs import CTSMsg, Trades, TopLevel, Book, RawBook, Candle

from thoth import DataNode

XPUB_ADDR = 'tcp://127.0.0.1:3998'
XSUB_ADDR = 'tcp://127.0.0.1:3999'
DEBUG_ADDR = 'tcp://127.0.0.1:3997'


class DataNodeTests(unittest.TestCase):

    @staticmethod
    def _connect_to_proxy():
        ctx = zmq.Context()
        sock = ctx.socket(zmq.SUB)
        sock.connect(XPUB_ADDR)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        return ctx, sock

    @staticmethod
    def _disconnect_from_proxy(ctx, socket):
        socket.close()
        ctx.term()

    def test_DN_publishes_on_raw(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        # Publish on raw
        with dn:
            dn.publish_raw('test_data')
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'raw/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
        self.assertEqual(cts_msg.data, 'test_data')

    def test_DN_publishes_on_ob(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        data = ([('1000', '10', '1'), ('999', '9', '2'), ('998', '8', '3')],
                [('1001', '11', '4'), ('1002', '12', '5'), ('1003', '12', '5')], '6')
        # Publish on raw
        with dn:
            dn.publish_book('TESTPAIR', data)
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'Book/TESTPAIR/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
        self.assertIsInstance(cts_msg.data, Book)
        self.assertIsInstance(cts_msg.data.bids, (tuple, list))
        for bid in cts_msg.data.bids:
            self.assertIsInstance(bid, (tuple, list), msg="Quote %s in bids of %s does not have "
                                                          "expected dtype!" % (bid, cts_msg.data))
            self.assertEqual(len(bid), 3, msg="Quote %s in bids of %s does not have "
                                              "expected length!" % (bid, cts_msg.data))
            for x in bid:
                self.assertIsInstance(x, str, msg="Element in bid %s  bids of %s does not have "
                                                  "expected dtype!" % (bid, cts_msg.data))

        self.assertIsInstance(cts_msg.data.asks, (tuple, list))
        for ask in cts_msg.data.asks:
            self.assertIsInstance(ask, (tuple, list), msg="Quote %s in asks of %s does not have "
                                                          "expected dtype!" % (ask, cts_msg.data))
            self.assertEqual(len(ask), 3, msg="Quote %s in asks of %s does not have "
                                              "expected length!" % (ask, cts_msg.data))
            for x in ask:
                self.assertIsInstance(x, str, msg="Element in ask %s  bids of %s does not have "
                                                  "expected dtype!" % (x, cts_msg.data))

    def test_DN_publishes_on_raw_ob(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        data = ([('1000', '10', 'uuid-1', '1'),
                 ('999', '9', 'uuid-2', '2'),
                 ('998', '8', 'uuid-3', '3')],
                [('1001', '11', 'uuid-4', '4'),
                 ('1002', '12', 'uuid-4', '5'),
                 ('1003', '12', 'uuid-5', '5')],
                '6')
        # Publish on raw
        with dn:
            dn.publish_raw_book('TESTPAIR', data)
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'RawBook/TESTPAIR/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
        self.assertIsInstance(cts_msg.data, RawBook)
        self.assertIsInstance(cts_msg.data.bids, (tuple, list))
        for bid in cts_msg.data.bids:
            self.assertIsInstance(bid, (tuple, list), msg="Quote %s in bids of %s does not have "
                                                          "expected dtype!" % (bid, cts_msg.data))
            self.assertEqual(len(bid), 4, msg="Quote %s in bids of %s does not have "
                                              "expected length!" % (bid, cts_msg.data))
            for x in bid:
                self.assertIsInstance(x, str, msg="Element in bid %s  bids of %s does not have "
                                                  "expected dtype!" % (bid, cts_msg.data))

        self.assertIsInstance(cts_msg.data.asks, (tuple, list))
        for ask in cts_msg.data.asks:
            self.assertIsInstance(ask, (tuple, list), msg="Quote %s in asks of %s does not have "
                                                          "expected dtype!" % (ask, cts_msg.data))
            self.assertEqual(len(ask), 4, msg="Quote %s in asks of %s does not have "
                                              "expected length!" % (ask, cts_msg.data))
            for x in ask:
                self.assertIsInstance(x, str, msg="Element in ask %s  bids of %s does not have "
                                                  "expected dtype!" % (x, cts_msg.data))

    def test_DN_publishes_on_toplevel(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        data = (('1000', '10', '1'), ('1001', '11', '4'), '6')

        # Publish on raw
        with dn:
            dn.publish_top_level('TESTPAIR', data)
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'TopLevel/TESTPAIR/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
        self.assertIsInstance(cts_msg.data, TopLevel)
        bid, ask, ts = cts_msg.data.bid, cts_msg.data.ask, cts_msg.data.ts
        self.assertIsInstance(cts_msg.data.bid, tuple, msg="Bid does not have valid dtype!")
        self.assertEqual(len(cts_msg.data.bid), 3, msg="Bid in %s does not have expected length!" %
                                                       cts_msg.data)
        self.assertIsInstance(cts_msg.data.ask, tuple, msg="Ask does not have valid dtype!")
        self.assertEqual(len(cts_msg.data.ask), 3, msg="Ask in %s does not have expected length!" %
                                                       cts_msg.data)
        self.assertIsInstance(cts_msg.data.ts, str, msg="Timestamp does not have valid dtype!")
        for item in bid:
            self.assertIsInstance(item, str, msg="Item in Bid %s does not have "
                                                 "expected dtype!" % (bid,))
        for item in ask:
            self.assertIsInstance(item, str, msg="Item in Ask %s does not have "
                                                 "expected dtype!" % (ask,))

    def test_DN_publishes_on_candles(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        data = ('5', '10', '1', '4', '1000')
        # Publish on raw
        with dn:
            dn.publish_candle('TESTPAIR', data)
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'Candle/TESTPAIR/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
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

    def test_DN_publishes_on_trades(self):
        fake_publisher = mocked_publisher()
        dn = DataNode("TestNode", mocked_wss(), fake_publisher)
        data = [('BTCUSD', '1000', '10', 'bid', 'uuid-1', None, '1'),
                ('BTCUSD', '999', '9', 'bid', 'uuid-2', '--someSpecialFlag', '2'),
                ('BTCUSD', '998', '8', 'ask', 'uuid-3', None, '3')]
        # Publish on raw
        with dn:
            dn.publish_trades(data)
        # assert that the publisher's publish() method was called.
        self.assertTrue(fake_publisher.publish.called)
        cts_msg = fake_publisher.publish.call_args[0][0]
        self.assertIsInstance(cts_msg, CTSMsg)
        self.assertEqual(cts_msg.topic, 'Trades/TestNode')
        self.assertEqual(cts_msg.origin, 'TestNode')
        self.assertIsInstance(cts_msg.data, Trades)
        for trade, expected_trade in zip(cts_msg.data.trades, data):
            self.assertIsInstance(trade, (tuple, list))
            self.assertEqual(trade, list(expected_trade))


if __name__ == '__main__':
    unittest.main(verbosity=2)

