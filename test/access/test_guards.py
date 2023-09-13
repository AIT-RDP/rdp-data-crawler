"""
Tests the service guards
"""
import datetime
import threading

import pytest

import data_crawler.access.guards as guards


def test_sequential_timing_guard_concurrent_access():
    """Tests accessing the SequentialTimingGuard concurrently"""

    guard = guards.SequentialTimingGuard(0.5)

    access_log = []
    access_log_lock = threading.Lock()

    def access_once():
        guard.wait_until_safe("test")
        with access_log_lock:
            access_log.append(datetime.datetime.utcnow())

    n_threads = 6
    threads = [threading.Thread(target=access_once) for _ in range(n_threads)]

    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert len(access_log) == n_threads
    for i in range(1, len(access_log)):
        assert access_log[i - 1] + datetime.timedelta(seconds=0.45) <= access_log[i], f"Access of log {i} too close"
