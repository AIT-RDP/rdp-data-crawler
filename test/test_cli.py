"""
Tests the high-level CLI and its configuration utilities

These test cases are mostly there to ensure compatibility with the AIT RDP library
"""

import os
from typing import Dict
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

import pyrdp_commons.cli as cli
import data_crawler.cli


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

    config = cli.setup_app(minimal_config_file, None)
    assert config is not None
    assert "version" in config
    assert config["version"] == 1

    assert "data sources" in config
    assert config["data sources"] is not None


def test_load_config_env_template(minimal_config_file, minimal_env_test_set):
    """Tests the environment variable_substitution"""

    config = cli.setup_app(minimal_config_file, None)

    assert "testing" in config
    assert "key" in config["testing"]

    assert "nsa:backdoor" == config["testing"]["key"]


@pytest.fixture()
def table_config_file() -> str:
    """Returns the path to a minimal configuration"""

    file_path = os.path.join(__file__, "../../data/test/test-config-tables.yml")
    file_path = os.path.abspath(file_path)
    return file_path


def test_load_config_table_csv(table_config_file, minimal_env_test_set):
    """Test loading an externally provided CSV table"""

    import pandas as pd  # May not be always available

    config = cli.setup_app(table_config_file, None)
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


# ========== Tests for cli.fetch command ==========

@pytest.fixture()
def mock_redis_pool():
    """Returns a mocked redis connection pool"""
    return MagicMock()


@pytest.fixture()
def cli_runner():
    """Returns a Click CLI test runner"""
    return CliRunner()


@patch('data_crawler.cli._load_redis_connection_pool')
@patch('data_crawler.query_executors.execute_one_shot_batches')
def test_fetch_no_filters_no_overrides(mock_execute, mock_redis, cli_runner, minimal_config_file, minimal_env_test_set):
    """Test fetch command without filters or overrides"""
    mock_redis.return_value = MagicMock()
    mock_execute.return_value = {}

    result = cli_runner.invoke(data_crawler.cli.cli, ['-c', minimal_config_file, 'fetch', 'my_datasource_name'])

    assert result.exit_code == 0
    mock_execute.assert_called_once()

    # Check the arguments passed to execute_one_shot_batches
    call_args = mock_execute.call_args
    filter_configs = call_args[0][2]  # Third positional argument
    source_names = call_args[0][3]    # Fourth positional argument
    override_config = call_args[1]['override_config']  # Keyword argument

    assert filter_configs == [{}]  # Empty filter
    assert list(source_names) == ['my_datasource_name']
    assert override_config == {}


@pytest.mark.parametrize("filter_args,expected_filters", [
    (["-f", "key1=value1"], [{"key1": "value1"}]),
    (["-f", "key1=value1", "-f", "key2=value2"], [{"key1": "value1", "key2": "value2"}]),
    (["-f", "key1=val1a", "-f", "key1=val1b", "-f", "key2=val2a", "-f", "key2=val2b"],
     [{"key1": "val1a", "key2": "val2a"}, {"key1": "val1b", "key2": "val2b"}]),
])
@patch('data_crawler.cli._load_redis_connection_pool')
@patch('data_crawler.query_executors.execute_one_shot_batches')
def test_fetch_with_filters(mock_execute, mock_redis, cli_runner, minimal_config_file, minimal_env_test_set,
                            filter_args, expected_filters):
    """Test fetch command with various filter expressions"""
    mock_redis.return_value = MagicMock()
    mock_execute.return_value = {}

    result = cli_runner.invoke(data_crawler.cli.cli, ['-c', minimal_config_file, 'fetch'] + filter_args + ['my_datasource_name'])

    assert result.exit_code == 0
    mock_execute.assert_called_once()

    call_args = mock_execute.call_args
    filter_configs = call_args[0][2]

    assert filter_configs == expected_filters


@pytest.mark.parametrize("override_args,expected_override", [
    (["-o", "key1=value1"], {"key1": "value1"}),
    (["-o", "nested.key=value"], {"nested": {"key": "value"}}),
    (["-o", "a.b.c=value"], {"a": {"b": {"c": "value"}}}),
    (["-o", "key1=val1", "-o", "key2=val2"], {"key1": "val1", "key2": "val2"}),
    (["-o", "parent.child1=val1", "-o", "parent.child2=val2"],
     {"parent": {"child1": "val1", "child2": "val2"}}),
])
@patch('data_crawler.cli._load_redis_connection_pool')
@patch('data_crawler.query_executors.execute_one_shot_batches')
def test_fetch_with_overrides(mock_execute, mock_redis, cli_runner, minimal_config_file, minimal_env_test_set,
                              override_args, expected_override):
    """Test fetch command with various override expressions"""
    mock_redis.return_value = MagicMock()
    mock_execute.return_value = {}

    result = cli_runner.invoke(data_crawler.cli.cli, ['-c', minimal_config_file, 'fetch'] + override_args + ['my_datasource_name'])

    assert result.exit_code == 0
    mock_execute.assert_called_once()

    call_args = mock_execute.call_args
    override_config = call_args[1]['override_config']

    assert override_config == expected_override


@patch('data_crawler.cli._load_redis_connection_pool')
@patch('data_crawler.query_executors.execute_one_shot_batches')
def test_fetch_with_list_overrides(mock_execute, mock_redis, cli_runner, minimal_config_file, minimal_env_test_set):
    """Test fetch command with list override syntax using [index]"""
    mock_redis.return_value = MagicMock()
    mock_execute.return_value = {}

    result = cli_runner.invoke(data_crawler.cli.cli,
                               ['-c', minimal_config_file, 'fetch',
                                '-o', 'items.[0].name=first',
                                '-o', 'items.[1].name=second',
                                'my_datasource_name'])

    assert result.exit_code == 0
    mock_execute.assert_called_once()

    call_args = mock_execute.call_args
    override_config = call_args[1]['override_config']

    expected = {"items": [{"name": "first"}, {"name": "second"}]}
    assert override_config == expected


@patch('data_crawler.cli._load_redis_connection_pool')
@patch('data_crawler.query_executors.execute_one_shot_batches')
def test_fetch_combined_filters_and_overrides(mock_execute, mock_redis, cli_runner, minimal_config_file, minimal_env_test_set):
    """Test fetch command with both filters and overrides"""
    mock_redis.return_value = MagicMock()
    mock_execute.return_value = {}

    result = cli_runner.invoke(data_crawler.cli.cli,
                               ['-c', minimal_config_file, 'fetch',
                                '-f', 'station=HTL-1',
                                '-o', 'polling.frequency=1h',
                                'my_datasource_name'])

    assert result.exit_code == 0
    mock_execute.assert_called_once()

    call_args = mock_execute.call_args
    filter_configs = call_args[0][2]
    override_config = call_args[1]['override_config']

    assert filter_configs == [{"station": "HTL-1"}]
    assert override_config == {"polling": {"frequency": "1h"}}

