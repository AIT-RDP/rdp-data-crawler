"""
Specifies the main interface for generic data sinks

A data sink is a passive destination for data that will be triggered as soon as new data is available. In addition to
the actual data that is to be placed at the destination, a set of meta-data items can be added that dynamically alters
the behaviour of the sink. To enforce construction parameters, each sink will be generated using a factory function. The
interface itself is highly inspired by the data bridge interface
from https://gitlab-intern.ait.ac.at/ees/rdp/generic-components/rdp-data-bridge/-/blob/main/rdp_data_bridge/sinks/abstract_sink.py?ref_type=heads
"""
import abc
from typing import Any

import pydantic


class SinkParameters(pydantic.BaseModel):
    """
    Base class for the sink parameter model
    """


class SinkMetadata(pydantic.BaseModel):
    """
    Base class for metadata expected by the sink
    """


class AbstractSinkAPI(abc.ABC):
    """
    Specifies the abstract data sink API to push data to.
    """

    @classmethod
    @abc.abstractmethod
    def create(cls, sink_parameters: SinkParameters, **kwargs) -> "AbstractSinkAPI":
        """
        Factory method to create a sink instance.

        This is needed because the :class:`~data_bridge.executor.Executor` expects a certain parameter signature to
        initialize the sinks.

        :param sink_parameters: Parameters of the sink. The parameters will be of the configuration type returned by the
            parameter_model() function.
        :param kwargs: Additional parameters and shared services. (For future compatibility)
        :return: Initialized sink instance
        """

        raise NotImplementedError

    @abc.abstractmethod
    def insert_data(self, data: dict[str, Any], metadata: SinkMetadata) -> None:
        """
        Inserts data received from an external source into a sink

        The data to be sent must be formatted using common conventions. Each observation needs to be represented by a
        unique key. In case multiple values are fetched as one, they need to be encapsulated in python lists. Multiple
        messages will be passed on using multiple function calls.

        :param data: The data to be inserted.
        :param metadata: Additional metadata
        """

        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def parameter_model() -> type[SinkParameters]:
        """
        Pydantic model for the parameters the sink expects.

        Derive from :class:`SinkParameters` to create a model for a new sink

        :return: Pydantic model for the sink parameters
        """

        raise NotImplementedError

    @staticmethod
    def metadata_model() -> type[SinkMetadata]:
        """
        Pydantic model for the metadata that can be handled by the data sink

        Derive from :class:`SinkMetadata` to create a model for a new sink and override the static method. Per default,
        no metadata are supported.

        :return: Pydantic model for the sink metadata
        """

        return SinkMetadata
