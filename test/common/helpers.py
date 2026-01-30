"""
Some cross-module testing helpers that help to write test cases and fixtures
"""

import json
import os

import pytest


def get_json_fixture(path: str):
    """
    Returns a fixture that reads the JSON file relative to the project directory
    :param path: The relative path to the json file
    :return: A pytest fixture
    """

    file = os.path.join(__file__, "../../", path)
    file = os.path.abspath(file)

    @pytest.fixture()
    def fixture():
        with open(file, "r") as f:
            data = json.load(f)
        return data

    return fixture


def to_string_values(config):
    """Convert every float/int value that is not a standard container to a string value and returns it"""

    if isinstance(config, dict):
        return {k: to_string_values(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [to_string_values(v) for v in config]
    elif any(isinstance(config, t) for t in [int, float]):
        return str(config)
    else:
        return config


def direct_config(config):
    """Just a simple identity function to make test runs more verbose"""
    return config
