# RDP Data Crawler

The RDP Data Crawler mainly interfaces external systems and the AIT RDP. It either periodically or event-driven fetches 
data from various sources such as forecasts or measurement information and stores the data into Redis streams. In 
addition, the AIT Data Crawler can push data to external systems such as Modbus or OPC UA devices. 

## Installation and System Integration

The RDP Data Crawler is designed to be integrated as Docker container into the AIT RDP. The main docker image is 
available at Docker Hub via `ait1/rdp-data-crawler`. In addition to version tags, the following are supported:
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
    image: ait1/rdp-data-crawler:latest-dev
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

## Configuration Structure

The configuration is done via a YAML file that supports the AIT RDP extensions such as variables substitution and 
templating. The configuration is organized into multiple channels that are described from the perspective of the data 
sources. For each channel, a single source is created and the data from that source is accessed, processed and written 
to a destination. In most cases, either the destination (default) or the source will be an internal Redis. However, 
also different configurations are supported. Hence, a single data crawler process can handle multiple sources 
concurrently and consequently reduces the overhead of spinning up a large amount of containers. The basic structure of 
the configuration is as follows:

```yaml
version: 1  # Configuration file version, mostly to ensure later compatibility

# List of sources that will spin up a dedicated channel for each source
data sources:
  source.name.0:  # Unique name of the channel. Mostly used for debugging
    type: <source_type>  # Type of the source to instantiate
    source parameter: {}  # Source-specific parameters are defined here
    # The polling section describes the timing of passive data sources that are executed periodically. Active data 
    # sources that listen on external events may emit messages at any time. Hence, the polling section can be omitted 
    # for these sources.
    polling:
      frequency: 5min
    
    sink_type: <sink_type>  # Type of the sink
    sink_parameters: {}  # Sink-specific parameters are defined here
    # Optionally attach sink metadata to each payload under the `_metadata` key.
    metadata:
      output: false  # When true, sink metadata (e.g. initial, force_initial) is nested under `_metadata`

  source.name.1: # Another source
    # ...
```

Each section has at lest a source and some sink configuration. Both sources and sinks are dynamically loaded by the
respective type. Source- and sink-specific parameters can be passed on in the `source parameter` and `sink_parameters`
sections, respectively.

In addition, the optional channel-level `metadata` section controls whether sink metadata is included in the payload.
When `metadata.output` is `true` (default: `false`), validated sink metadata is nested under the `_metadata` key of
each payload. The executor also adds the fields `initial` (true only for the first push of a channel) and `force_initial` 
(taken from `polling.force_initial`, default `false`) to the payload's `_metadata` node.

In case a Redis sink is used, the configuration can be simplified by omitting the `sink_type` and `sink_parameters`
sections and appending a `redis` section, instead:

```yaml
version: 1
data sources:
  source.name.0:
    type: <source_type>
    source parameter: {}
    polling:
      frequency: 5min
    # The Redis section replaces the sink_type and sink_parameters sections.
    redis:
      stream: <stream_name>  # Name of the Redis stream to write to.
      tags:  # Optional tags to be added to the Redis stream.
        message-key-0: message-value-0
        message-key-1: message-value-1
```

Note that the redis sink allows to append arbitrary but static message fields. This can be used to set meta-data that 
is needed for further data processing.

In order to simplify repeating configurations and share a common redis connection pool, the redis server information is 
outsourced to a common root-level `redis` section as follows.

```yaml
version: 1
data sources: []  # The source configuration as described above

redis:  # Common Redis server configuration
  host: redis.example.local # The hostname or IP address of the Redis server
  port: 6379  # The port of the Redis server
  password: ${REDIS_PASSWORD}  # The password to access the Redis server. Can be left out, if no password is set.
  db: 0  # The Redis database index to use
```

## Polling Configuration

Sources that require regular polling can be configured via the `polling` section. In order to determine the timing and 
avoid overloading various sources, a fine-grained control is possible. At minimum, the `frequency` parameter must be 
set. The complete list of parameters is as follows:

* `frequency`: The nominal interval between two consecutive polling operations. Values are interpreted according to the 
  [Pandas Timedelta Specification](https://pandas.pydata.org/pandas-docs/stable/user_guide/timedeltas.html) including 
  ISO 8601 duration representation. Examples are `5min`, `1h`, `1d`, and `1w`.
* `jiter`: A uniformly distributed random jitter that is added to the nominally scheduled time. This may be useful to 
  load balance a source serving multiple requests at the same time. The value is interpreted according to the 
  [Pandas Timedelta Specification](https://pandas.pydata.org/pandas-docs/stable/user_guide/timedeltas.html) including 
  ISO 8601 duration representation. The jitter is applied symmetrically, i.e., the scheduled point in time may be both 
  reduced or extended by the jitter value, at maximum.
* `offset`: A fixed offset to shift the scheduling interval. Per default, scheduling is aligned to the full 
  hour/day/month/etc. The offset shifts this alignment by the given timedelta. Values are represented as described 
  above.
* `slot count`, `slot id`: The scheduling interval can be divided into multiple time slots, where each slot is occupied 
  by another operation. This feature can, for instance, be used to access devices by multiple data crawlers in a 
  round-robin fashion avoiding concurrent data access. `slot count` defines the total number of slots, while `slot id` 
  gives the current slot of the source process. The slot IDs start with 0 and max out at `slot count - 1`. Note that the
  slot system only affects the scheduling offset and does not perform any synchronization among sources or data 
  crawlers. Make sure that sufficient timing reserves are available and that the system time among multiple hosts is 
  sufficiently well synchronized.
  Per default, one slot is configured.
* `force initial`: If set to `true`, the source will be triggered immediately after startup. This behaviour is mainly 
  intended to quickly populate the AIT RDP data structures on startup without the need of waiting a long time until 
  regular scheduling intervals (e.g., of forecasts) are met. Defaults to `true`.

In case the processing duration exceeds the configured frequency, the next operation will be immediately scheduled. If 
the delay exceeds the following regular interval, the triggering point will be skipped in order to avoid pile-up of 
delays and unpredictable timing.

## Execution Live-Cycle
Each channel is executed independently. In case a stale or faulty channel is detected, a restart is riggered to clear 
transient faults and support for less stable, external sources. In addition, the status as well as any restarts are 
recorded and exposed via detailed prometheus metrics. Per default, the prometheus port 8000 and metric path '/' is 
used. A prometheus scrape config therefore may be as follows:

```yaml
  - job_name: data-crawler
    metrics_path: '/'
    static_configs:
      - targets:
        - data-crawler:8000
```

In order to support development setups that do not directly write to the final assets, a dry-run mode is provided. 
For each channel, the `dry_run` parameter can be set to `true` to avoid writing to the final sink. Instead, the 
data is dropped after processing without passing it on to any sink. Note that for safety reasons, the dry-run flag, 
if configured, must not be left emty. Hence, disabling dry run requires passing on a dediated `false` value. Per 
default, `dry_run` is disabled. The following configuration snippet shows an externally supplied `dry_run` indicated 
via a corresponding environment variable.

```yaml
  controller.output.battery-0:
    # Suppress writing to the final sink if the environment variable DATA_CRAWLER_DRY_RUN_WRITES is set to true.
    dry_run: ${DATA_CRAWLER_DRY_RUN_WRITES}
    
    type: "data_crawler.sources.redis.RedisStream"
    source parameter: {} # ... the RedisStream source parameters
    sink_type: "data_crawler.sinks.modbus.ModbusTCP"
    sink_parameters: {} # ... the Modbus sink parameters
```

## Data Sources and Data Sinks

The default distribution of the AIT RDP Data Crawler already supports a broad variety of data sources and data sinks. 
The following overview lists the main ones. Detailed configurations can be found in the 
[data source and data sink description](docs/data_sources_and_data_sinks.md).

* Meteorological data
  * Weatherbit
    * `data_crawler.sources.weatherbit.CurrentWeather`: Current weather estimations (not recommended for archiving).
    * `data_crawler.sources.weatherbit.HourlyForecasts`: Hourly, numerical weather prediction data for a particular 
      location
  * met.no
    * `data_crawler.sources.yr_no.LocationForecast` Numerical weather prediction data for a particular location
  * Geosphere Austria
    * `data_crawler.sources.zamg.MeasurementStationData`: Live and historic measurements
    * `data_crawler.sources.zamg.NumericalWeatherPredictionData`: Numerical weather prediction data, both point 
      predictions and ensemble forecasts.
  * KNMI
    * `data_crawler.sources.knmi.WeatherStationsKNMI`: Weather station data

* Generic protocols and interfaces
  * `data_crawler.sources.modbus.ModbusTCP`: Modbus TCP source
  * `data_crawler.sinks.modbus.ModbusTCP`; Modbus TCP sink
  * `data_crawler.sources.opc_ua.OPCUA`: OPC UA source
  * `data_crawler.sinks.opc_ua.OPCUA` OPC UA sink
  * `data_crawler.sinks.redis.RedisStream`: Redis stream sink (default)
  * `data_crawler.sources.teltonika_modbus.TeltonikaModbus`: REST interface to receive Modbus data via Teltonica devices
  * `data_crawler.sources.influxdb.InfluxDBSource`: InfluxDB source to read time series data from an InfluxDB instance

* Energy- and market-related services:
  * ENTSO-E
    * `data_crawler.sources.entsoe_da.ENTSOEDATransparency`: Day-ahead market prices from ENTSO-E
  * Fingrid
    * `data_crawler.sources.fingrid.DatasetData`: Timeseries data from Fingrid Open Data

* Device-specific interfaces
  * Fronius
    * `data_crawler.sources.fronius.FroniusInverterRealtimeData`: Device-level real-time data from Fronius inverters
    * `data_crawler.sources.fronius.FroniusInverterPowerFlowRealtimeData`: Real-time power-flow data of all devices 
      connected to the data logger
    * `data_crawler.sources.fronius.FroniusSystemArchiveData`: Device-level API to query historic values and detailed
      information from Fronius inverters
