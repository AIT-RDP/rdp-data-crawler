"""
Implements some mockup sources used for testing
"""
import asyncio
import itertools
import threading
import datetime
import time
from typing import Optional, Any, Dict, Generator, Union, Iterable, AsyncIterator

import pydantic
import pyrdp_commons as commons

import data_crawler.sources.abc.abstract_source as abstract_sources
import data_crawler.sources.abc.active_source_sync as active_source_sync
import data_crawler.sources.abc.message as msg
import data_crawler.sources.abc.history as history


class PassiveMockupSourceAPI(abstract_sources.AbstractSourceAPI, history.AbstractTimedHistorySourceMixin):
    """Passive test API source that just counts function invocations"""

    def __init__(self, source_parameters, metadata: Optional[dict[str, Any]] = None, **kwargs):
        """Stores the configuration and initializes the object"""
        self.kwargs = kwargs

        self.config = source_parameters
        self.fetch_invocations = 0
        self.history_invocations = 0
        self.start_invocations = 0
        self.stop_invocations = 0

        self.fetch_ts = []

        self.enable_fetch = threading.Event()
        self.enable_fetch.set()
        self._metadata = metadata

    def start(self):
        """Counts the start and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations

        if self.config.get("raise_on_startup", False):
            raise ValueError("Uups!")

        self.start_invocations += 1

    def fetch_data(self) -> msg.MessageData:
        """Generates some content and returns it"""

        self.fetch_ts.append(datetime.datetime.now(tz=datetime.timezone.utc))
        self.fetch_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.fetch_invocations % 2 == 1:
            raise ValueError("That's odd.")

        self.enable_fetch.wait()

        payload = {
            "invocations": self.fetch_invocations,
            "data": "some-test-nonsense",
            "duplicate": "api-key"
        }
        if self._metadata is not None:
            return msg.Message(payload=payload, metadata=self._metadata)
        else:
            return payload

    def fetch_historic_data(self, start_time: datetime.datetime, end_time: datetime.datetime,
                            filter_clauses: Dict[str, Any]) -> Dict[str, Any]:
        """Generates some content and returns it"""

        self.history_invocations += 1

        assert self.start_invocations == self.stop_invocations + 1

        if self.config.get("no odd invocations", False) and self.history_invocations % 2 == 1:
            raise ValueError("That's odd.")

        return {
            "fetch_invocations": self.fetch_invocations,
            "history_invocations": self.history_invocations,
            "data": "another-test-nonsense",
            "duplicate": "same-api-key"
        }

    def stop(self):
        """Counts the stop and performs some basic checks"""
        assert self.start_invocations == self.stop_invocations + 1
        self.stop_invocations += 1


class ActiveMockupSourceParameters(active_source_sync.SourceParameters):
    """Defines the configuration parameters for the active source"""

    sleep_time: float = pydantic.Field(default=0.1)
    repeat: bool = pydantic.Field(default=True)
    messages: list[dict] = pydantic.Field(default=[dict(message="got it")])
    track_activity: bool = pydantic.Field(default=False)


class ActiveMockupSourceAPI(active_source_sync.AbstractSyncActiveSourceAPI):
    """
    Mockup source that follows the active source API specification

    Each mockup source has a sequence counter that is used to count function invocations
    """

    def __init__(self, config: ActiveMockupSourceParameters):
        """Initializes the object counters"""

        super().__init__()
        self._invocation_counter = 0

        # The lists of observed counter values. For each invocation, the current counter will be added
        self.start_exec = []
        self.run_exec = []
        self.run_message_exec = []
        self.shutdown_exec = []
        self.stop_exec = []

        self._last_wakeup = None
        self._last_cycle_complete = None

        self._config = config
        self._termination_request = threading.Event()

        self.enable_run = threading.Event()
        self.enable_run.set()

    @property
    def config(self) -> ActiveMockupSourceParameters:
        """Returns the encapsulated configuration for testing purpose"""
        return self._config

    def draw_invocation_counter(self) -> int:
        """Increases the counter value and returns previous one"""
        ret = self._invocation_counter
        self._invocation_counter += 1
        return ret

    @classmethod
    def create(cls, source_parameters: ActiveMockupSourceParameters,
               **kwargs) -> active_source_sync.AbstractSyncActiveSourceAPI:
        return cls(config=source_parameters)

    def start(self) -> None:
        self.start_exec.append(self.draw_invocation_counter())
        self._termination_request.clear()

    def run(self) -> Generator[msg.MessageData, None, None]:
        """Releases the messages until the termination request is received or all messages are exhausted"""

        self.run_exec.append(self.draw_invocation_counter())

        if self._config.repeat:
            messages = itertools.cycle(self._config.messages)
        else:
            messages = self._config.messages

        for message in messages:
            self._last_wakeup = datetime.datetime.now(tz=datetime.timezone.utc)

            self.enable_run.wait()  # Allow to block the run method to simulate errors
            if self._termination_request.is_set():
                break
            self.run_message_exec.append(self.draw_invocation_counter())

            yield message
            self._last_cycle_complete = datetime.datetime.now(tz=datetime.timezone.utc)

            time.sleep(self._config.sleep_time)

    def shutdown(self) -> None:
        self.shutdown_exec.append(self.draw_invocation_counter())
        self._termination_request.set()

    def stop(self) -> None:
        self.stop_exec.append(self.draw_invocation_counter())

    def get_activity_status(self) -> active_source_sync.ActivityStatus:
        if self.config.track_activity:
            return active_source_sync.ActivityStatus(
                last_wakeup=self._last_wakeup, last_cycle_complete=self._last_cycle_complete,
                max_permitted_cycle_time=datetime.timedelta(seconds=self.config.sleep_time)
            )
        else:
            return active_source_sync.ActivityStatus(last_wakeup=None, last_cycle_complete=None,
                                                     max_permitted_cycle_time=None)

    @staticmethod
    def parameter_model() -> type[ActiveMockupSourceParameters]:
        return ActiveMockupSourceParameters


class StaticConfigSequenceTemplate(commons.AbstractTemplate):
    """
    Implements a configuration template to induce configuration changes

    To indicate a new realization, a dedicated event will be fired
    """

    def __init__(self, realizations: Union[Iterable[dict], Iterable[list]]):
        """
        Sequentially maps to the individual realizations

        :param realizations: The realization to materialize one after another. If the realizations are lists, the
        template may be used in a list context. Likewise, if the individual elements are dicts, they may be used in a
        dict context.
        """

        self._realizations = list(realizations)
        self._current_content: Union[list, dict] = self._realizations.pop(0)
        self.event_trigger = asyncio.Semaphore(0) # Release to trigger new events

    async def expand_to_mapping(self, key_template_node) -> dict:
        assert isinstance(self._current_content, dict)
        return self._current_content

    async def expand_to_sequences(self) -> list:
        assert isinstance(self._current_content, list)
        return self._current_content

    async def subscribe(self) -> AsyncIterator[commons.ChangeEvent]:
        """Delivers the change events"""
        while len(self._realizations) > 0:
            await self.event_trigger.acquire()  # Wait to trigger new events

            self._current_content = self._realizations.pop(0)
            event = commons.ChangeEvent(commons.ChangeEventType.UPDATE, None, None, self)
            yield event