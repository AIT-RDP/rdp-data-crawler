"""
Provides some utilities to coordinate multithreadded access to a single service

The classes mainly target the coordination of some legacy services that do not allow concurrent access
"""
import datetime
import threading
import time
from typing import Union


class SequentialTimingGuard:
    """
    Guards and coordinates access to limited services that require to meet common timing constraints

    The class is thread save and can be accessed concurrently. It differentiates between different nominal targets,
    however does not take aliases and name resolution into account.
    """

    def __init__(self, min_period: Union[float, datetime.timedelta]):
        """
        Initializes the guard

        :param min_period: The minimum duration between two concurrent access attempts
        """

        if not isinstance(min_period, datetime.timedelta):
            min_period = datetime.timedelta(seconds=float(min_period))
        self._min_period = min_period

        self._last_access = {}
        self._last_access_lock = threading.Lock()

    def wait_until_safe(self, service_id):
        """
        Checks whether it is safe to access the service and waits until it is safe.

        Note that the function just checks the service within the local process and does not coordinate among data
        crawler processes.

        :param service_id: The unique id for the given service or device
        """

        while True:
            ts_now = datetime.datetime.utcnow()
            with self._last_access_lock:
                remaining_time = ts_now - self._last_access.get(service_id, datetime.datetime.utcfromtimestamp(0.0))
                if remaining_time > self._min_period:
                    self._last_access[service_id] = ts_now
                    break
            time.sleep(remaining_time.total_seconds())
