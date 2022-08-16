"""
Specifies the abstract source API to query external services
"""

import abc
from typing import Dict, Any


class AbstractSourceAPI(abc.ABC):
    """
    Specifies the interface of one data source that can be dynamically instantiated and executed to query data

    Each AbstractSourceAPI object will be dynamically instantiated based on the system configuration. The configuration
    is thereby passed on to the __init__ function via keyword arguments. To ensure upwards comparability, it is highly
    advised to catch all keyword arguments via **kwargs and only use the relevant ones. The following initialization
    arguments are supported:

      * source_parameters: Dict, The source-specific parameters listed in the configuration
      * executor_name: The name of the executor for debugging purpose
    """

    def __init__(self, **kwargs):
        """Just for type checking"""

    @abc.abstractmethod
    def fetch_data(self) -> Dict[str, Any]:
        """
        Fetches the data from the external API and returns it.

        The returned data must be formatted using common conventions. Each observation needs to be represented by a
        unique key. In case multiple values are fetched as one, they need to be encapsulated in python lists.

        :return: The results queried from the source API.
        """
