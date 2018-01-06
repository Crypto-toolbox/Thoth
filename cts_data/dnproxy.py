"""Provide the Data Cluster Proxy."""
# Import Built-Ins
import logging

# Import Homebrew
from hermes.proxy import ClusterInterfaceProxy
from hermes.utils import load_config_for

# Init Logging Facilities
log = logging.getLogger(__name__)


class DataClusterProxy(ClusterInterfaceProxy):
    """Initialize the Data Cluster Proxy."""

    # pylint: disable=too-few-public-methods

    def __init__(self):
        """Initialize the instance.

        We'll try to load preset using hermes.utils.load_config() - if we can't find any
        settings, we'll use the built-in defaults of the library.
        """
        proxy_in, proxy_out, debug_addr = load_config_for('DATA')

        super(DataClusterProxy, self).__init__(proxy_in, proxy_out, debug_addr)
