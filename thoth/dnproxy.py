"""Provide the Data Cluster Proxy."""
# Import Built-Ins
import logging

# Import Homebrew
from hermes.proxy import ClusterInterfaceProxy

# Init Logging Facilities
log = logging.getLogger(__name__)


class DataClusterProxy(ClusterInterfaceProxy):
    """Initialize the Data Cluster Proxy."""

    # pylint: disable=too-few-public-methods

    def __init__(self, proxy_pub_side, proxy_sub_side, debug_addr):
        """Initialize the instance."""
        super(DataClusterProxy, self).__init__(proxy_pub_side, proxy_sub_side, debug_addr)
