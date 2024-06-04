"""
Specifies the abstract source API to query external services
"""

import abc
from collections.abc import Generator
from typing import Dict, Any
from data_crawler.sources.abc.message import MessageData


class AbstractMultiMessageSourceAPI(abc.ABC):
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
    def fetch_data_bundle(self) -> Generator[MessageData, None, None]:
        """
        Fetches the remote data into a bundle of multiple messages.

        The function may be overridden in case multiple values need to be returned

        :returns: The function will return a generator that yields one message at a time.
        """

        yield {}

    def start(self):
        """
        Starts the operation of the data source

        The life-cycle hook is called within the executing thread before fetch_data() is accessed. The function may be
        used to connect to external resources and to initialize thread-sensitive APIs.
        """
        pass

    def stop(self):
        """
        Stops the source operation and frees allocated resources
        """
        pass


class AbstractSourceAPI(AbstractMultiMessageSourceAPI, abc.ABC):
    """
    Refines the abstract AbstractMultiMessageSourceAPI by a convenience hook that returns exactly one message at a time
    """

    def __init__(self, **kwargs):
        """Just for type checking"""
        super(AbstractSourceAPI, self).__init__(**kwargs)

    def fetch_data_bundle(self) -> Generator[MessageData, None, None]:
        """
        Fetches the remote data into a bundle of multiple messages.

        The function may be overriden in case multiple values need to be returned

        :returns: The function will return a generator that yields one message at a time.
        """

        data = self.fetch_data()
        if data is not None:
            yield data

    @abc.abstractmethod
    def fetch_data(self) -> MessageData:
        """
        Fetches the data from the external API and returns it.

        The returned data must be formatted using common conventions. Each observation needs to be represented by a
        unique key. In case multiple values are fetched as one, they need to be encapsulated in python lists.

        :return: The results queried from the source API.
        """
