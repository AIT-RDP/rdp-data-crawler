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