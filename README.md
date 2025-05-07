# RDP Data Crawler

The RDP Data Crawler mainly interfaces external systems and the AIT RDP. It either periodically or event-driven fetches 
data from various sources such as forecasts and measurement information and stores the data into Redis streams. In 
addition, the AIT Data Crawler can push data to external systems such as Modbus or OPC UA devices. 

## Installation and System Integration

The RDP Data Crawler is designed to be integrated as Docker container into the AIT RDP. The main docker image is 
available via Docker Hub via `ait1/rdp-data-crawler`. In addition to version tags, the following are supported:
 * `latest`: The latest stable release branch.
 * `latest-dev`: The latest version of the development branch. 

Since the configurations are commonly rather complex, a direct configuration via environment variables is not feasible. 
Instead, a configuration file or directory is mounted. Per default, the configuration is located at 
`/etc/data_crawler/config.yml`. Nevertheless, the whole `/etc/data_crawler/` directory can be mounted in case 
sub-configuration files are needed. The following example shows a basic docker-compose service definition:

```yaml
services:
  # ...
  data-crawler:
    image: ait1/data-crawler:latest-dev
    volumes:
      - ./data-crawler/config.yml:/etc/data_crawler/config.yml:ro
    environment:
      REDIS_USERNAME: ${REDIS_USERNAME}
      REDIS_PASSWORD: ${REDIS_PASSWORD}
    depends_on:
      - redis
    restart: unless-stopped
```

For other installation methods, including custom data sources and development setups, please refer to the 
[Advanced Installation](docs/advanced_installation.md) section.
