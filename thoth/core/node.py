"""Basic Node for a data cluster."""

# pylint: disable=no-name-in-module,unexpected-keyword-arg,too-many-branches

# Import Built-Ins
import logging
import queue
from abc import abstractmethod

# Import Third-Party

# Import Home-grown
from hermes import Node, Envelope

# Init Logging Facilities
log = logging.getLogger(__name__)


class DataNode(Node):
    """Provides a connector interface and publisher."""

    # pylint: disable=too-many-instance-attributes

    @abstractmethod
    def process_frames(self, topic, data, ts):
        """Publish the given data to channel, if it is available.

        The object must implement a ``publish(envelope)`` method, otherwise a
        :exception:``NotImplementedError`` is raised.

        :param channel: topic tree
        :param data: Data Struct or string
        :return: :class:`None`
        """
        envelope = Envelope(topic, self.name, (data, ts))
        try:
            self.publisher.publish(envelope)
        except AttributeError:
            raise NotImplementedError

    # def pull(self, block=False, timeout=None):

    def run(self):
        """Execute main loop.

        For data to be processed, it is assumed to have a certain format when it is returned from
        the internal :attr:`thoth.DataNode.receiver` object, which is as follows:
            (topic, data, timestamp)

        Of course any other 3-item tuple would be valid as well; Once data is packaged by the
        Connector class, we do not touch it again.

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
