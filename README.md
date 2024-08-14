# RDP Data Crawler

Periodically fetches various data sources such as forecasts and measurement information and stores the data into Redis
streams.

## Poetry Development Setup

On Windows, one may want to manage the python interpreter versions using 
[pyenv-win](https://pyenv-win.github.io/pyenv-win/).
```shell
# List available interpreter versions
pyenv install -l

# Install and register the python interpreter
pyenv install 3.10.11  # Oldest supported version. You may want to choose a newer one
pyenv local 3.10.11  # Locally activate your python version
poetry env use $(pyenv which python)  # Create the poetry environment based on the selected interpreter
```

If you more into conda, you can install the base environment as follows. However, be aware that sometimes update 
problems due to the two package managers (poetry, conda) are reported. You are warned, here are the conda snippets:
```shell
conda create -n rdp-data-crawler python=3.11
conda activate rdp-data-crawler
conda install poetry
```

In case you have a dedicated conda environment that is not shared among poetry projects, make sure to directly install 
the packages within the conda environment. Otherwise, an additional virtualenv may be created which often creates 
troubles and redundancies.
```shell
poetry config --local virtualenvs.create false
```

Independent of your environment some dependencies are needed. In case your current user does not have access to the
[PyRDP Commons](https://gitlab-intern.ait.ac.at/ees/rdp/generic-components/pyrdp-commons) repository, use an access
token.
Replace `$TOKEN_PYRDP_COMMONS` with the value of the token.
Furthermore, this token is defined as a group variable of the GitLab group `EES/RDP`.
```shell
poetry config http-basic.gitlab-pyrdp-commons __token__ $TOKEN_PYRDP_COMMONS
```

Having your pyton/poetry base setup ready, one can install the development dependencies as follows.
```shell
# Make sure the correct conda environment is activated, if you have one. For poetry no further preparation is needed.
poetry install --with dev -E modbus  # The modbus libraries come with the modbus extras
```

## Run the test cases

To run the test cases, a development instance of Redis is needed. E.g. spin up one by using podman or docker:
```shell
podman run -p 6379:6379 -it docker.io/redis
```

To configure the parameters of the test suite, the following environment variables can be set:
```shell
REM Your e-mail to send to some public APIs that require contact details 
set DATA_CRAWLER_CONTACT="<contact details and e-mail>"
REM Information to access the Redis database (will create some test artefacts there):
set DATA_CRAWLER_REDIS_DB=0
set DATA_CRAWLER_REDIS_HOST=localhost
set DATA_CRAWLER_REDIS_PORT=6379
```

Furthermore, make sure that both the project dicrectory and the testing directory are in the PYTHONPATH. This can 
usually be done within the GUI by including the content roots and project directory or via the CLI:  
```shell
set PYTHONPATH=%PYTHONPATH%;.;./test
```

Have fun with testing:
```shell
pytest test
```

## Using the Data crawler with Project-Specific Sources

The data crawler is designed to include project-specific API bindings that are not part of the main repository. For such
cases, there are Python packages that encapsulate the main logic. To include the software in own poetry-managed 
projects, the dependencies need to be included as follows:

```shell
# Add the data source of pyrdp-commons, a core dependency of the data crawler
poetry source add -s gitlab-pyrdp-commons https://gitlab-intern.ait.ac.at/api/v4/projects/3611/packages/pypi/simple
poetry config http-basic.gitlab-pyrdp-commons __token__ ${TOKEN_PYRDP_COMMONS}

# Add the data source of the rdp-data-crawler itself 
poetry source add gitlab-rdp-data-crawler https://gitlab-intern.ait.ac.at/api/v4/projects/3040/packages/pypi/simple
poetry config http-basic.gitlab-rdp-data-crawler __token__ ${TOKEN_RDP_DATA_CRAWLER}

# Install the data crawler. If you need modbus support make sure that the modbus-crawler repository located at 
# https://gitlab-intern.ait.ac.at/ees-lachs/modbus-crawler is accessible and add the modbus extra with -E modbus
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
