"""
Implements a synchronous interface to an source that controls the event timing by itself

In contrast to the traditional abstract sources that will be triggered by an external timing, an active source listens
to new messages and returns them as soon as they are available.
"""

import abc
import dataclasses
import datetime
from typing import Generator, Optional

import pydantic

import data_crawler.sources.abc.message as msg


@dataclasses.dataclass()
class ActivityStatus:
    """
    Groups some execution and activity status information to monitor the source and restart it, if needed
    """

    # The datetime of the last wakeup that triggers fetching or processing external information.
    last_wakeup: Optional[datetime.datetime]
    # The datetime when the last processing cycle was completed.
    last_cycle_complete: Optional[datetime.datetime]

    # The maximum allowed time, between two event emissions. In case no such limit can be given, None is to be returned
    max_permitted_cycle_time: Optional[datetime.timedelta]


class SourceParameters(pydantic.BaseModel):
    """
    Base class for the source parameter model
    """


class AbstractSyncActiveSourceAPI(abc.ABC):
    """
    Specifies the interface for an active source

    To specify the factory interface, a dedicated factory function is specified that needs to be overridden.
    """

    @classmethod
    @abc.abstractmethod
    def create(cls, source_parameters: SourceParameters | dict, **kwargs) -> "AbstractSyncActiveSourceAPI":
        """
        Factory function that creates an ActiveSyncSourceAPI
        :param source_parameters: The source parameters according to the parameter model. In case no parameter model is
            given the plain dictionary must be given for compatibility reasons.
        :param kwargs: Any additional shared services
        :return: The newly created source API
        """

        raise NotImplementedError()

    def __init__(self):
        """Initializes the internal state of the source API interface"""

        self._is_termination_req = False

    def start(self) -> None:
        """
        Live-cycle hook that will be called before run or any other mixin function is executed.

        The function will be called from the same thread as run. It is mainly intended for compatibility reason with
        external mixins that may require a dedicated startup phase. Overwrite this function at your will.
        """
        pass

    @abc.abstractmethod
    def run(self) -> Generator[msg.MessageData, None, None]:
        """
        Executes the source and expects the messages to be returned as they arrive

        The generator must exit as soon as a shutdown request is issued. To give it the chance of cleaning up
        allocated resources, the calling loop will not be stopped unless the corresponding end-of-iteration exception is
        raised. It is guaranteed that the function is always called by the same thread to manage thread-bound resources.
        :return: Yields the messages as they arrive
        """

        raise NotImplementedError()

    @abc.abstractmethod
    def shutdown(self) -> None:
        """
        Indicates that the run generator must stop

        The function will be called by another thread at any time. It will trigger the source-internal shutdown
        procedure. The function may directly return without waiting for the successful shutdown. The shutdown sequence
        is complete when both, the run function terminates and the stop() function returns. In case run() is not
        executed, shutdown may also not be executed.
        """
        
        raise NotImplementedError()

    def stop(self) -> None:
        """
        Live-cycle hook that completes the termination sequence

        The function will always be called after a successful start operation. In case run() is triggered, the
        shutdown() function will be called before stop(). However, in case execution control is not transferred to the
        run() function, only stop may be triggered.
        """

        pass

    def get_activity_status(self) -> ActivityStatus:
        """
        Returns the current activity status of the source

        The function may be called by any thread. Hence, make sure to avoid any race conditions. Although it is not
        mandatory to implement the status interface, it is highly adviced to be able to gracefully restart the source
        in case it fails. In case it is not overloaded, a default status without any information is returned.

        :return: The current status information on the source activity
        """

        return ActivityStatus(last_wakeup=None, last_cycle_complete=None, max_permitted_cycle_time=None)

    @staticmethod
    @abc.abstractmethod
    def parameter_model() -> type[SourceParameters]:
        """
        Pydantic model for the parameters the source expects.

        Derive from :class:`SourceParameters` to create a model for a new sink.

        :return: Pydantic model for the sink parameters
        """

        raise NotImplementedError
