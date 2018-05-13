"""Data structs for use within the hermes ecosystem."""

import logging
import json
import time
import sys
from functools import reduce


log = logging.getLogger(__name__)


class ThothEnvelope:
    """Extension of :cls:`hermes.Envelope` message transport class.

    Transport Object for data being sent between :mod:`Thoth` components via ZMQ.

    Serializes and deserializes date to and from json.

    It tracka topic and origin of the data it transport, as well as the
    timestamp it was last updated at. Updates occur automatically whenever
    :meth:`Thoth.ThothEnvelope.convert_to_frames` is called.
    This timestamp can be used to detect Slow-Subscriber-Syndrome by :class:`hermes.Receiver` and
    to initiate the suicidal snail pattern.
    """

    __slots__ = ['topic', 'origin', 'data', 'ts']

    def __init__(self, topic_tree, origin, data, ts=None):
        """Initialize a :class:`thoth.ThothEnvelope` instance.

        .. Note::
            The attribute :attr:`thoth.ThothEnvelope.ts` denotes the UNIX timestamp at which this
            instance was last serialized - it is NOT to be confused with the timestamp of the data
            it carries!

        :param topic_tree: topic this data belongs to
        :param origin: the sender of this message (Publisher)
        :param data: data struct transported by this instance
        :param ts: timestamp of this instance, defaults to current unix ts if
                   None
        """
        self.topic = topic_tree
        self.origin = origin
        self.data = data
        self.ts = ts or time.time()

    def __repr__(self):
        """Construct a basic string-represenation of this class instance."""
        return ("Envelope(topic=%r, origin=%r, data=%r, ts=%r)" %
                (self.topic, self.origin, self.data, self.ts))

    @staticmethod
    def load_from_frames(frames, encoding=None):
        """
        Load json to a new :class:`thoth.ThothEnvelope` instance.

        Automatically converts to string if the passed object is
        a :class:`bytes.encode()` object.

        :param frames: Frames, as received by :meth:`zmq.socket.recv_multipart`
        :param encoding: The encoding to use for :meth:`bytes.encode()`; default UTF-8
        :return: :class:`thoth.ThothEnvelope` instance
        """
        encoding = encoding if encoding else 'utf-8'
        topic, origin, data, ts = [json.loads(x.decode(encoding)) for x in frames]

        return ThothEnvelope(topic, origin, data, ts)

    def convert_to_frames(self, encoding=None):
        """
        Encode the :class:`thoth.ThothEnvelope` attributes as a list of json-serialized strings.

        :param encoding: the encoding to us for :meth:`str.encode()`, default UTF-8
        :return: list of :class:`bytes`
        """
        self.update_ts()
        return [json.dumps(x.encode(encoding or 'utf-8'))
                for x in (self.topic,self.origin, self.data, self.ts)]

    def update_ts(self):
        """Update the :attr:`thoth.ThothEnvelope.ts` attribute with the current UNIX time."""
        self.ts = time.time()
