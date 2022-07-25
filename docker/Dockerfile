FROM python:3.10
# Do not use the alpine images as it is missing many dependencies

WORKDIR /usr/src/app

COPY requirements.txt setup.py setup.cfg setup.py versioneer.py ./
COPY data_crawler ./data_crawler
COPY docker/etc/data_crawler /etc/data_crawler

RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir .

CMD ["datacrawler", "--config", "/etc/data_crawler/config.yml", "--env", "/etc/data_crawler/default.env"]