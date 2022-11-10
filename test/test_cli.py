"""
Tests the high-level CLI and its configuration utilities
"""

import os
from typing import Dict

import pytest

import data_crawler.cli as cli


@pytest.fixture()
def minimal_config_file() -> str:
    """Returns the path to a minimal configuration"""

    file_path = os.path.join(__file__, "../../data/test/minimal-config.yml")
    file_path = os.path.abspath(file_path)
    return file_path


@pytest.fixture()
def mockup_env_file() -> str:
    """Returns a simple mockup .env file"""

    file_path = os.path.join(__file__, "../../data/test/mockup.env")
    file_path = os.path.abspath(file_path)
    return file_path


@pytest.fixture()
def minimal_env_test_set() -> Dict[str, str]:
    """Temporarily updates the environment variables and returns the managed set"""

    env_set = {
        "API_KEY": "backdoor",
        "API_USER": "nsa"
    }

    backup_set = {key: os.environ.get(key, None) for key in env_set.keys()}

    os.environ.update(env_set)
    yield env_set

    # Revert the changes to the environment variables to keep debugging sane
    for var_name, var_value in backup_set.items():
        if var_value is None:
            del os.environ[var_name]
        else:
            os.environ[var_name] = var_value


def test_load_config_minimal(minimal_config_file, minimal_env_test_set):
    """Loads and checks the minimal test config"""

    config = cli.load_config(minimal_config_file)
    assert config is not None
    assert "version" in config
    assert config["version"] == 1

    assert "data sources" in config
    assert config["data sources"] is not None


def test_load_config_env_template(minimal_config_file, minimal_env_test_set):
    """Tests the environment variable_substitution"""

    config = cli.load_config(minimal_config_file)

    assert "testing" in config
    assert "key" in config["testing"]

    assert "nsa:backdoor" == config["testing"]["key"]


@pytest.fixture()
def table_config_file() -> str:
    """Returns the path to a minimal configuration"""

    file_path = os.path.join(__file__, "../../data/test/test-config-tables.yml")
    file_path = os.path.abspath(file_path)
    return file_path


def test_load_config_table_csv(table_config_file):
    """Test loading an externally provided CSV table"""

    import pandas as pd  # May not be always available

    config = cli.load_config(table_config_file)
    assert config["standard mapping"]["hello"] == "world"

    reference = pd.DataFrame({
        "Register_start": [100, 110],
        "Register_end": [100, 111],
        "Register_type": ["i", "i"],
        "Data_type": ["INT16", "SINGLE"],
        "Name": ["I_L1", "f"],
        "Unit": ["A", "Hz"],
        "Scaling": [0.01, 1]
    })

    assert isinstance(config["table 1"], pd.DataFrame)
    pd.testing.assert_frame_equal(config["table 1"], reference)

    assert isinstance(config["table 2"], pd.DataFrame)
    pd.testing.assert_frame_equal(config["table 2"], reference)


def test_load_env_mockup(mockup_env_file, minimal_env_test_set):
    """Tests loading the environment file"""

    cli.load_env_file(mockup_env_file)  # No override of existing variables
    assert os.environ["API_KEY"] == "backdoor"
    assert os.environ["API_USER"] == "nsa"

    del os.environ["API_KEY"]
    del os.environ["API_USER"]

    cli.load_env_file(None)
    assert "API_KEY" not in os.environ
    assert "API_USER" not in os.environ

    cli.load_env_file(mockup_env_file)
    assert os.environ["API_KEY"] == "not-a-real-pwd"
    assert os.environ["API_USER"] == "database-host"
