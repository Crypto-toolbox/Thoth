import logging
import unittest.mock as mock

import pytest

from thoth.core import DataNode
from hermes import Publisher, Receiver


@pytest.fixture
def dummy_node():
    """Mock DataNode for testing the run method."""
    class DummyDataNode(DataNode):
        def process_frames(self, topic, data, ts):
            pass

    def return_params(*args, **kwargs):
        return args, kwargs

    def return_increment(*args, **kwargs):
        counter = 0
        for x in iter(int, 1):
            yield 'Test', 'topic data', counter
            counter += 1
    
    fake_pub = mock.Mock(spec=Publisher)
    fake_recv = mock.Mock(spec=Receiver)
    mock_node = DummyDataNode('TestDataNode', publisher=fake_pub, receiver=fake_recv)
    mock_node.process_frames = mock.Mock(side_effect=return_params)
    mock_node.recv = mock.Mock(side_effect=return_increment)
    return mock_node


def test_process_frames():
    class TestDataNode(DataNode):
        def process_frames(self, topic, data, ts):
            super(TestDataNode, self).process_frames(topic, data, ts)
    fake_pub = mock.Mock(spec=Publisher)
    fake_recv = mock.Mock(spec=Receiver)
    node = TestDataNode('TestDataNode', publisher=fake_pub, receiver=fake_recv)
    node.publish = mock.Mock()

    node.process_frames("topic", "this is data", 12345)
    node.publish.assert_called_once_with("topic", ("this is data", 12345))


"""
Test the DataNode class' run method.

Cases:
    Case 1:
        Node.receiver.recv() raises an queue.Empty exception when we call Node.recv()
        :Expected result: The Node instance keeps on running
    Case 2:
        Node.receiver.recv() raises a TimeoutError exception when we call Node.recv()
        :Expected result: The Node instance keeps on running
    Case 3:
        The unpacking of frames raises a ValueError, because there were either too few or too
        many arguments to unpack.
        :Expected result: The exception is logged, and the frames as well; the Node instance 
        keeps on running
    Case 4:
        The frames are received and unpacked successfully, and Node.process_frames is called
        :expected result: Node.process_frames is called with a topic, data and ts parameter 
        equivalent to the defined return value of a mocked Node.recv() call.
"""


def test_case_1_queue_empty_exception(dummy_node):
    pytest.fail("Finish this test!")


def test_case_2_timeout_exception_on_recv_call():
    pytest.fail("Finish this test!")


def test_case_3_value_error_on_tuple_unpacking():
    pytest.fail("Finish this test!")


def test_case_4_recv_and_unpack_successful_result_in_process_frames_called():
    pytest.fail("Finish this test!")
