import logging
import unittest.mock as mock

import pytest

from thoth.core import DataNode
from hermes import Publisher, Receiver

@pytest.fixture
def dummy_node():
    class DummyDataNode(DataNode):
        def process_frames(self, topic, data, ts):
            pass

    def return_params(*args, **kwargs):
        return args, kwargs
    fake_pub = mock.Mock(spec=Publisher)
    fake_recv = mock.Mock(spec=Receiver)
    mock_node = DummyDataNode('TestDataNode', publisher=fake_pub, receiver=fake_recv)
    mock_node.process_frames = mock.Mock(side_effect=return_params)