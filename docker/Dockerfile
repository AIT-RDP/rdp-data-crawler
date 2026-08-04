ARG DOCKER_PROXY_PREFIX
# Do not use the alpine images as it is missing many dependencies
FROM ${DOCKER_PROXY_PREFIX}python:3.10 AS builder

# Keep the uv version in sync with the UV_VERSION variable in .gitlab-ci.yml
ARG UV_VERSION=0.12.1
RUN pip install --no-cache-dir uv==${UV_VERSION}

ENV UV_PROJECT_ENVIRONMENT=/app/.venv \
    UV_CACHE_DIR=/tmp/uv_cache \
    UV_LINK_MODE=copy \
    UV_NO_PROGRESS=1

WORKDIR /app

# Install the locked dependencies only. The project itself is not installed, since its sources are copied into the
# runtime image directly. All third-party dependencies are publicly available, hence no repository credentials are
# required anymore.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --all-extras --no-install-project && \
    rm -rf ${UV_CACHE_DIR}

# Build the runtime image
FROM ${DOCKER_PROXY_PREFIX}python:3.10-slim

# Use the environment variables instead of arguments to specify the config and .env file. This should make both
# customization and invoking subcommands easier. Additionally, the venv is set without having to use uv
ENV DATA_CRAWLER_CONFIG="/etc/data_crawler/config.yml" \
    DATA_CRAWLER_ENV="/etc/data_crawler/default.env" \
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}
COPY docker/etc/data_crawler /etc/data_crawler
COPY data_crawler ./data_crawler

CMD ["python", "-m", "data_crawler.cli", "run"]
