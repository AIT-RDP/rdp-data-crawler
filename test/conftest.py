"""
Configures unit tests globally
"""

import os
import sys

import pytest


@pytest.fixture()
def appended_test_path() -> str:
    """Appends the test directory to the system path and returns that path"""

    test_dir = os.path.abspath(os.path.join(__file__, ".."))
    sys.path.append(test_dir)

    yield test_dir

    assert sys.path[-1] == test_dir
    sys.path.pop(-1)
