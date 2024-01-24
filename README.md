# E³ Data Crawler

Periodically fetches publicly available forecast and measurement information and stores it into Redis streams.

## Poetry Development Setup

On Windows, one may want to setup a clean conda environment to install poetry. Alternatively, a generic poetry 
installation may be used.
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

Having your pyton/poetry base setup ready, one can install the development dependencies as follows.
```shell
poetry install  # Make sure the correct conda environment is activated, if you have one
```

## Legacy Development setup (Windows)

Create and activate the conda development environment (Contains some development packages that are not needed for 
productive usage):
```shell
conda env create -f .\environment-win.yml
conda activate e3-data-crawler
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

For development purpose, the application can be directly executed by referencing the data_crawler module.
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

