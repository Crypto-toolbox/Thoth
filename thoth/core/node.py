"""Basic Node for a data cluster."""

# pylint: disable=no-name-in-module,unexpected-keyword-arg,too-many-branches

# Import Built-Ins
import logging
import queue
from abc import abstractmethod

# Import Third-Party

# Import Home-grown
from hermes import Node

# Init Logging Facilities
log = logging.getLogger(__name__)


class DataNode(Node):
    """Provides a connector interface and publisher."""

    # pylint: disable=too-many-instance-attributes

    @abstractmethod
    def process_frames(self, topic, data, ts):
        """Process the frames received by the Connector."""
        self.publish(topic, (data, ts))

    def pull(self, block=False, timeout=None):

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
                frames = self.recv(block=False, timeout=3)
            except (TimeoutError, queue.Empty):
                continue
            try:
                topic, data, ts = frames
            except ValueError as e:
                log.exception(e)
                log.error(frames)
                continue
            self.process_frames(topic, data, ts)
