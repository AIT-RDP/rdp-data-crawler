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

  source.name.1: # Another source
    # ...
```

Each section has at lest a source and some sink configuration. Both sources and sinks are dynamically loaded by the 
respective type. Source- and sink-specific parameters can be passed on in the `source parameter` and `sink_parameters` 
sections, respectively. In case a Redis sink is used, the configuration can be simplified by omitting the `sink_type` 
and `sink_parameters` sections and appending a `redis` section, instead:

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

* Energy- and market-related services:
  * ENTSO-E
    * `data_crawler.sources.entsoe_da.ENTSOEDATransparency`: Day-ahead market prices from ENTSO-E

* Device-specific interfaces
  * Fronius
    * `data_crawler.sources.fronius.FroniusInverterRealtimeData`: Device-level real-time data from Fronius inverters
    * `data_crawler.sources.fronius.FroniusInverterPowerFlowRealtimeData`: Real-time power-flow data of all devices 
      connected to the data logger
    * `data_crawler.sources.fronius.FroniusSystemArchiveData`: Device-level API to query historic values and detailed
      information from Fronius inverters