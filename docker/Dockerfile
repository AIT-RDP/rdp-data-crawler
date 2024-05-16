# Do not use the alpine images as it is missing many dependencies
FROM ${DOCKER_PROXY_PREFIX}python:3.10 as builder

COPY requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Define credential to access the modbus_crawler repo. Note that the CI_JOB_TOKEN will not work in the external process
ARG MODBUS_GIT_USER
ARG MODBUS_GIT_PASSWORD
ARG TOKEN_PYRDP_COMMONS

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN touch README.md

RUN poetry config http-basic.gitlab-pyrdp-commons __token__ ${TOKEN_PYRDP_COMMONS} && \
    poetry config repositories.gitlab-intern-ees-lachs-modbus-crawler https://gitlab-intern.ait.ac.at/ees-lachs/modbus-crawler && \
    poetry config http-basic.gitlab-intern-ees-lachs-modbus-crawler ${MODBUS_GIT_USER} ${MODBUS_GIT_PASSWORD} && \
    poetry install --without dev --all-extras --no-root && \
    rm -rf $POETRY_CACHE_DIR

# Build the runtime image
FROM ${DOCKER_PROXY_PREFIX}python:3.10-slim

# Use the environment variables instead of arguments to specify the config and .env file. This should make both
# customization and invoking subcommands easier. Additionally, the venc is set without having to use poetry
ENV DATA_CRAWLER_CONFIG="/etc/data_crawler/config.yml" \
    DATA_CRAWLER_ENV="/etc/data_crawler/default.env" \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY docker/etc/data_crawler /etc/data_crawler
COPY data_crawler ./data_crawler

CMD ["python", "-m", "data_crawler.cli", "run"]