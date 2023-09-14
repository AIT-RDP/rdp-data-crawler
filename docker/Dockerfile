# Do not use the alpine images as it is missing many dependencies
FROM python:3.10

# Define GIT credential to access the modbus_crawler repo
ARG MODBUS_GIT_USER
ARG MODBUS_GIT_PASSWORD

WORKDIR /usr/src/app

COPY requirements.txt setup.py setup.cfg setup.py versioneer.py ./
COPY data_crawler ./data_crawler
COPY docker/etc/data_crawler /etc/data_crawler

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

# Use the environment variables instead of arguments to specify the config and .env file. This should make both
# customization and invoking subcommands easier.
ENV DATA_CRAWLER_CONFIG="/etc/data_crawler/config.yml" DATA_CRAWLER_ENV="/etc/data_crawler/default.env"
CMD ["datacrawler", "run"]