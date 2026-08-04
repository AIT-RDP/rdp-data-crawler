# Advanced Installation

The RDP Data Crawler is mainly designed to be integrated as Docker container into AIT RDP setups. Nevertheless, it also 
supports an integration as python dependency, in case custom data sources should be implemented. The following section 
describes the development setup and custom data source installations. 

## Development Setup with uv

The project is managed with [uv](https://docs.astral.sh/uv/). The dependency set is pinned in `uv.lock`, which is 
committed to the repository and must be kept in sync with `pyproject.toml`. Make sure that uv is properly installed.
Afterwards, the complete development environment is created with a single command. uv creates a dedicated virtual 
environment in `.venv`, downloads a matching python interpreter if none is available and installs the locked 
dependencies:
```shell
# Install all extras and the development dependencies into .venv
uv sync --all-extras
```

The interpreter version is taken from the `.python-version` file, which pins the oldest supported version. To develop 
against another version, pin it explicitly. uv manages the interpreters itself, so neither pyenv nor conda is needed:
```shell
uv python pin 3.12  # Writes .python-version and is picked up by the next uv sync
uv sync --all-extras
```

Only a subset of the dependencies is needed in most cases. The extras can therefore be selected individually, and the 
development dependencies can be skipped entirely:
```shell
uv sync --extra modbus  # The modbus libraries come with the modbus extras
uv sync --all-extras --no-dev  # Runtime dependencies only
```

Commands are executed within the environment via `uv run`, which implicitly keeps `.venv` in sync with `uv.lock`. 
Alternatively, the environment can be activated as usual with `.venv\Scripts\activate` respectively 
`source .venv/bin/activate`:
```shell
uv run datacrawler --help
uv run python -m data_crawler.cli --help
```

## Run the test cases

To run the test cases, a development instance of Redis is needed. The MQTT and InfluxDB test cases require a broker 
and an InfluxDB 2 instance respectively, and are skipped if the corresponding environment variables are unset. All 
three services can be started via docker or podman:
```shell
docker run -d -p 6379:6379 docker.io/redis
docker run -d -p 1883:1883 -p 8883:8883 docker.io/eclipse-mosquitto:2.1-alpine
docker run -d -p 8086:8086 -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_ORG=ait ^
    -e DOCKER_INFLUXDB_INIT_BUCKET=my-bucket -e DOCKER_INFLUXDB_INIT_USERNAME=admin ^
    -e DOCKER_INFLUXDB_INIT_PASSWORD=<password> docker.io/influxdb:2
```

The parameters of the test suite are configured via environment variables. All supported variables are documented in 
[`.env.example`](../.env.example), which is the recommended starting point:
```shell
copy .env.example .env
```

The `.env` file is read automatically by the VSCode test explorer and the debug configurations in `.vscode`. On the 
command line, the variables have to be exported by the shell, since the test suite does not read `.env` on its own:
```shell
REM Your e-mail to send to some public APIs that require contact details 
set DATA_CRAWLER_CONTACT="<contact details and e-mail>"
REM Information to access the Redis database (will create some test artefacts there):
set DATA_CRAWLER_REDIS_DB=0
set DATA_CRAWLER_REDIS_HOST=localhost
set DATA_CRAWLER_REDIS_PORT=6379
```

No `PYTHONPATH` setup is required anymore. Both the project directory and the test directory are registered via the 
`pythonpath` option in the `[tool.pytest.ini_options]` section of `pyproject.toml`, which applies to the command line, 
the CI pipeline and the IDE alike.

Have fun with testing:
```shell
uv run pytest test

REM Including the coverage report of the data_crawler package
uv run pytest --cov --cov-report term --cov-report html:htmlcov test
```

In VSCode, the test cases are discovered by the native test explorer and can be executed and debugged from the sidebar 
or the gutter icons. The `Pytest: All Test Cases`, `Pytest: Current File` and `Pytest: All Test Cases with Coverage` 
entries of the run and debug view provide the same via `F5`. Both rely on the interpreter in `.venv`, so make sure 
`uv sync --all-extras` has been executed before.

## Using the Data crawler with Project-Specific Sources

The data crawler is designed to include project-specific API bindings that are not part of the main repository. For such
cases, there are Python packages that encapsulate the main logic. The crawler itself is published to the GitLab package
registry, whereas its own git-based dependencies (`pyrdp-commons` and, for the extras, `modbus-crawler` and `rdp-mqtt`)
are recorded as direct references in the package metadata and are resolved from their public repositories.

To include the software in an own uv-managed project, register the registry as an additional index. The credentials are
supplied via the `UV_INDEX_<NAME>_USERNAME` and `UV_INDEX_<NAME>_PASSWORD` environment variables, where `<NAME>` is the
upper-case index name:
```shell
# Add the index of the rdp-data-crawler and install the package. If you need modbus support, add the modbus extra
# with --extra modbus.
set UV_INDEX_GITLAB_RDP_DATA_CRAWLER_USERNAME=__token__
set UV_INDEX_GITLAB_RDP_DATA_CRAWLER_PASSWORD=%TOKEN_RDP_DATA_CRAWLER%
uv add --index gitlab-rdp-data-crawler=https://gitlab-intern.ait.ac.at/api/v4/projects/3040/packages/pypi/simple rdp-data-crawler
```

The equivalent declaration in the `pyproject.toml` of the consuming project looks as follows:
```toml
[[tool.uv.index]]
name = "gitlab-rdp-data-crawler"
url = "https://gitlab-intern.ait.ac.at/api/v4/projects/3040/packages/pypi/simple"
explicit = true  # Only used for the packages that are explicitly assigned to it

[tool.uv.sources]
rdp-data-crawler = { index = "gitlab-rdp-data-crawler" }
```

For poetry-managed consumer projects, the corresponding commands are:
```shell
poetry source add gitlab-rdp-data-crawler https://gitlab-intern.ait.ac.at/api/v4/projects/3040/packages/pypi/simple
poetry config http-basic.gitlab-rdp-data-crawler __token__ ${TOKEN_RDP_DATA_CRAWLER}
poetry add --source gitlab-rdp-data-crawler rdp-data-crawler
```

## Executing the Data Crawler 

For development purpose and to develop own setups, the application can be directly executed by referencing the 
`data_crawler` module. For all other setups, the corresponding docker container is recommended.
```
(e3-data-crawler) C:\Users\Me\Projekte\E3-SCHOOL\e3-data-crawler>python data_crawler --help
usage: data_crawler [-h] [--config_file CONF] [--env ENV_FILE]

Periodically fetches the data sources

options:
  -h, --help          show this help message and exit
  --config_file CONF  The main YAML configuration describing the data sources
  --env ENV_FILE      An environment file that specifies the variables to load
```

Since there is no extensive documentation on the configuration formats, please refer to the project configurations, 
e.g. at the [E3 Docker Repository](https://gitlab-intern.ait.ac.at/ees/rdp/e3-at-school/e3-docker/-/blob/main/e3-data-crawler/config.yml)  
