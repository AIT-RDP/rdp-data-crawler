# Data Sources and Data Sinks

The AIT RDP Data Crawler is mainly driven by configurable sources and sinks that access remote systems. This section 
describes the main configuration options.

## Meteorological data

### Weatherbit
#### Current Weather
**Interface Type**: Source

**Type Name**: `data_crawler.sources.weatherbit.CurrentWeather`

**Description**: The current weather API fetches the actual state estimations from the Weatherbit API. It is not 
recommended by Weatherbit to use these values for archive purpose.

**Parameters**
* `api key`: The API key to access the Weatherbit API. This key is required for all Weatherbit APIs.
* `latitude`: The latitude of the location to be queried.
* `longitude`: The longitude of the location to be queried.

#### Hourly Forecasts
**Interface Type**: Source

**Type Name**: `data_crawler.sources.weatherbit.HourlyForecasts`

**Description**: The hourly forecast source fetches the hourly forecast data from the Weatherbit API.

**Parameters**
* `api key`: The API key to access the Weatherbit API. This key is required for all Weatherbit APIs.
* `latitude`: The latitude of the location to be queried.
* `longitude`: The longitude of the location to be queried.
* `horizon hours`: The number of hours in the future to be queried. The maximum value returned by the API is 240 
  hours (default).

### Meteorologisk Institutt Norway - met.no
#### Location Forecast
**Interface Type**: Source

**Type Name**: `data_crawler.sources.yr_no.LocationForecast`

**Description**: The location forecast source fetches the numerical weather prediction data from the met.no API for a 
single location.

**Parameters**
* `latitude`: The latitude of the location to be queried.
* `longitude`: The longitude of the location to be queried.
* `altitude`: The altitude of the location to be queried. If none is given, the default ground altitude as induced by 
  the API is used.
* `contact address`: A contact address that should be sent along with the API request. This is required by the met.no API 
  and should be a valid email address. The address is used to contact you in case of problems with the API.

### Geosphere Austria
#### Measurement Station Data
**Interface Type**: Source

**Type Name**: `data_crawler.sources.zamg.MeasurementStationData`

**Description**: The measurement station data source fetches the live and historic measurements from the Geosphere 
measurement stations. There are two dedicated endpoint types. TAWES and climate. The first one returns the real-time 
information with less quality control and a shorter historic timeframe and the later returns the quality controlled 
measurements. Note that both endpoints use different station IDs and therefore may not be easily exchanged. Please 
consider the [climate data](https://data.hub.geosphere.at/dataset/klima-v1-10min) and 
[TAWES data](https://data.hub.geosphere.at/dataset/tawes-v1-10min) documentation for further details on the data 
sources and station IDs.

**Parameters**
* `station id`: The station ID to be queried. Note that the station is different for the TAWES and climate endpoints.
* `initial history`: The past duration to fetch data from. After the initial query, only new samples will be returned. 
  Defaults to 48h.
* `endpoint`: The name of the endpoint to be used. Either `TAWES`, `climate-v2`, or `climate` (deprecated). The 
  default is `climate`, but this will be elevated to `climate-v2` in future releases.
* `data points`: A list of data points to be fetched. The data point nomenclature corresponds to the Geosphere naming  
  and not the AIR RDP names. Please consider the Geosphere documentation for further details. Per default, all supported
  data points will be added.

#### Numerical Weather Prediction Data
**Interface Type**: Source

**Type Name**: `data_crawler.sources.zamg.NumericalWeatherPredictionData`

**Description**: The numerical weather prediction data source fetches the weather forecasts from the Geosphere API. The 
source supports two endpoints, a standard numeric weather prediction data that return a single value for each 
observation and an ensemble forecast that returns some percentiles in addition.

**Parameters**:
* `latitude`: The latitude of the location to be queried.
* `longitude`: The longitude of the location to be queried.
* `endpoint`: The name of the endpoint to be used. Either `NWP` or `ensemble`. The default is `NWP`.
* `data points`: A list of data points to be fetched. The data point nomenclature corresponds to the Geosphere naming
  and not the AIR RDP names. Please consider the Geosphere documentation for further details. Per default, all supported
  data points will be added.

### KNMI

#### Weather Stations
**Interface Type**: Source

**Type Name**: `data_crawler.sources.knmi.WeatherStationsKNMI`

**Description**: The KNMI weather stations source fetches the live and historic measurements from the KNMI weather 
stations. For accessing the dataset, an [API Key](https://developer.dataplatform.knmi.nl/open-data-api#token) is 
required.

**Parameters**
* `api_key`: The API key to access the KNMI API.
* `stations`: A list of station IDs or a dict-based configuration having an `id` attribute listing the station IDs.
* `initial_history`: The past duration to fetch data from. After the initial query, only new samples will be returned. 
  The default history for KNMI weather stations is 12h.
* `drop_missing_observations`: Drop observations that do not contain any valid values. Per default, all returned 
  observations are included, even if they have just NaN values.


## Generic protocols and interfaces
#### Modbus TCP
**Interface Type**: Source and Sink

**Type Name**: `data_crawler.sources.modbus.ModbusTCP` and `data_crawler.sinks.modbus.ModbusTCP`

**Description**: The Modbus TCP source and sink fetches the data from a Modbus TCP server and writes dedicated message 
fields back. Right now, sink and source are separated and maintain one Modbus TCP connection, each. If this is an issue 
(e.g., due to single-connection servers), please open a ticket on GitHub.

**Parameters**:
* `address`: The address of the Modbus TCP server.
* `port`: The port of the Modbus TCP server. Defaults to 502.
* `register_spec`: The register specification to be used. This is a list of dictionaries that define the register 
  addresses and types. Instead of directly defining the register specification, also an (external CSV) table can be  
  loaded and processed. If an external table is used, make sure that the column names are properly defined. In general, 
  the following columns or dict-entries are supported:
  * `register_start`: The start address of the register. This is a required field, however, for some rows, it may be 
    intentionally left empty or NaN. In case an empty or NaN value is observed, the register will be fetched in one 
    block with the previous one and a consecutive addressing is assumed.
  * `name`: The name of the data point to be fetched. This is a required field. The name is used to create the message 
    field of the output message.
  * `data_type`: The data type of the register. This is also a required field and will determine the number of 
    consecutive registers to be read or written. The following data types are supported:
    * `int16`: 16-bit signed integer
    * `uint16`: 16-bit unsigned integer
    * `int32`: 32-bit signed integer
    * `uint32`: 32-bit unsigned integer
    * `float16`: 16-bit float
    * `float32`: 32-bit float
    * `float64`: 64-bit float
    * `stringN`: N-character ASCII String (e.g. `string16` for 16 characters). In case the string is null-terminated 
      before the maximum number of characters, it will be shortened. Similarly, it will be trimmed, in case it is 
      filled with space characters.
    * `bool`: Boolean value
  * `register_type`: The modbus type of the register. This is a required field and will determine the access method 
    (holding register, input register, etc.). The following types are supported:
    * `holdingreg`/`holdingregister`/`h`: Holding register
    * `inputreg`/`inputregister`/`i`: Input register
    * `coils`/`c`: Coils
    * `discreteinput`/`d`: Discrete input
  * `unit`: An optional unit description of the data point. This is not used for the Modbus TCP interface, but may be 
    useful for documentation purposes.
  * `scaling`: An optional scaling factor to be applied to the data point. In case an integer is scaled by a double, 
    then a double message field will be output. Otherwise, the original data type will be kept.
  * `used`: boolean flag that indicates whether the register is used or not. Per default, it is set to `true` and 
    therefore included in the output.
  * `unit_id`: The device ID of the Modbus TCP server. Per default, 1 is assumed.
  * `description`: A textual description of the data point. This is not used for the Modbus TCP interface, but may be 
    useful for documentation purposes as well.
  * `mode`: The access mode (`r` for reading, `w` for writing and `rw` for read/write). Per default, `r` is assumed. The
     mode modifier can be used to reference the same register description both for a source and a sink.
* `byte_order`: The byte order of the Modbus TCP server. Per default, big-endian is assumed. `>` indicates big-endian, 
  `<` indicates little-endian.
* `word_order`: The word order of the Modbus TCP server. Per default, big-endian is assumed. `>` indicates big-endian, 
  `<` indicates little-endian.

**Example**: For instance, the following configuration snippet defines a Modbus TCP source that reads power values from 
an electricity meter:

```yaml
  submeters.modbus.F3_104:
    type: "data_crawler.sources.modbus.ModbusTCP"
    source parameter:
      register spec: !table/csv
        path: "modbus/PAC2200_modbus_registers.csv"
      address: "10.0.3.104"
    polling:
      frequency: 10s
    redis:
      stream: "measurements.submeters.modbus.F1"
      tags:
        location_code: "GG2"
        data_provider: "PAC2200"
        device_name: "F3_104"
```

with the following register specification in `modbus/PAC2200_modbus_registers.csv`:

```csv
unit_id;Register_start;Register_end;Name;Data_type;Unit;Register_type;Scaling
1;63;72;S_tot;SINGLE;VA;i;1
;;;P_tot;SINGLE;W;i;1
;;;Q_tot;SINGLE;var;i;1
;;;PF_tot;SINGLE;-;i;1
```

#### MQTT
**Interface Type**: Source and Sink

**Type Name**: `data_crawler.sources.mqtt.MqttSource` and `data_crawler.sinks.mqtt.MqttSink`

**Description**: The MQTT source and sink enable communication with MQTT brokers for both subscribing to topics (source)
and publishing to topics (sink). The implementation uses the [RDP MQTT](https://github.com/AIT-RDP/rdp-mqtt) library which supports various payload formats including
JSON, compressed JSON (Zstandard), and Sparkplug B.

**Common Parameters**:
* `host`: The hostname or IP address of the MQTT broker. Defaults to `localhost`.
* `port`: The port of the MQTT broker. Standard ports are 1883 (no TLS) and 8883 (with TLS). Defaults to 8883.
* `username`: Optional username for broker authentication.
* `password`: Optional password for broker authentication.
* `ssl`: Boolean flag to enable/disable TLS encryption. Defaults to `true`.
* `validate_certificate`: Whether to validate the broker's TLS certificate. Defaults to `true`.
* `identifier`: Client identifier for the MQTT connection. Defaults to a randomly generated ID with prefix `RDP_`.
* `qos`: Quality of Service level (0, 1, or 2). Defaults to 0.
  * `0`: At most once delivery (fire and forget)
  * `1`: At least once delivery (acknowledged)
  * `2`: Exactly once delivery (assured)
* `payload_parser`: The parser to use for message payloads. Options: `json`, `json_zstd` (compressed JSON), or
  `sparkplug` (Sparkplug B protocol). Defaults to `json`.
* `field_precision`: Optional number of decimal places to round numeric fields, reducing payload size. Defaults to `None`
  (no rounding).

**Source-Specific Parameters**:
* `topic`: The MQTT topic to subscribe to. Supports wildcards:
  * `+`: Single-level wildcard (e.g., `sensors/+/temperature`)
  * `#`: Multi-level wildcard (e.g., `sensors/#`)
  * Default: `#` (all topics)

**Sink-Specific Parameters**:
* `topic`: The default MQTT topic to publish to. Can be overridden per message via metadata.
* `batch_size`: Number of metrics to batch together before sending. Set to 0 to disable batching. Defaults to 0.
* `batch_timeout`: Maximum time in seconds to wait before sending a partial batch. Defaults to 1.0 seconds.

**Sparkplug Parameters** (when using `payload_parser: sparkplug`):
* `sparkplug_group_id`: Sparkplug group ID for outgoing messages. Defaults to a randomly generated ID.
* `sparkplug_node_id`: Sparkplug node ID for outgoing messages. Defaults to a randomly generated ID.
* `sparkplug_device_id`: Sparkplug device ID for outgoing messages. Defaults to a randomly generated ID.

**Example**: The following configuration creates an MQTT source that subscribes to sensor data and an MQTT sink that
publishes processed data:

```yaml
version: 1

data sources:
  # MQTT Source: Subscribe to sensor data
  sensors.mqtt.input:
    type: "data_crawler.sources.mqtt.MqttSource"
    source parameter:
      host: "mqtt.example.com"
      port: 1883
      topic: "sensors/temperature/#"
      ssl: false
      qos: 1
      payload_parser: "json"
    sink_type: "data_crawler.sinks.redis.RedisStream"
    sink_parameters:
      stream: "sensors.raw"

  # MQTT Sink: Publish processed data
  sensors.mqtt.output:
    type: "data_crawler.sources.redis.RedisStream"
    source parameter:
      streams:
        - name: "sensors.processed"
          metadata: {}
      group_name: "mqtt_publisher"
      consumer_name: "publisher_1"
    sink_type: "data_crawler.sinks.mqtt.MqttSink"
    sink_parameters:
      host: "mqtt.example.com"
      port: 1883
      topic: "sensors/processed/data"
      ssl: false
      qos: 1
      batch_size: 10
      batch_timeout: 5.0

redis:
  host: localhost
  port: 6379
  db: 0
```

#### InfluxDB
**Interface Type**: Source

**Type Name**: `data_crawler.sources.influxdb.InfluxDBSource`

**Description**: The InfluxDB source fetches time-series data from an InfluxDB instance using Flux queries. The source 
maintains a rolling time window to fetch only new data in subsequent queries. Each table returned by the Flux query is 
transformed into a separate message, with columns mapped directly to message fields. The source supports batching of 
data fetches to handle large time ranges efficiently.

**Parameters**:
* `url`: The URL of the InfluxDB instance.
* `token`: The authentication token for InfluxDB.
* `org`: The organization name in InfluxDB.
* `query`: The Flux query to fetch the data. The query can use the parameters `_start_time` and `_stop_time` which are 
  automatically injected by the source to control the time range of the query.
* `initial_history`: The past duration to fetch data from on the first query. After the initial query, only new samples 
  will be returned. Defaults to 1 hour.
* `lag_time`: The lag time to account for late arriving data. The source will query data up to the current time minus 
  this lag time. Defaults to 0 minutes.
* `overlap`: Optional duration (default `0`) to extend each poll window backwards after the first poll, re-querying 
  the tail of the previous interval. Useful when late-arriving data may appear after the nominal poll boundary. Note 
  that overlapping polls return duplicate samples.
* `batch_duration`: Optional parameter to fetch data in batches of the specified duration. If not set, all data between 
  start and stop time is fetched in a single query. This is useful for handling large time ranges that might exceed 
  query limits or memory constraints.

**Example**: The following configuration snippet defines an InfluxDB source that reads temperature sensor data:

```yaml
  sensors.influxdb:
    type: "data_crawler.sources.influxdb.InfluxDBSource"
    source parameter:
      url: "https://influxdb.example.com"
      token: "${INFLUXDB_TOKEN}"
      org: "my-organization"
      query: |
        from(bucket: "sensors")
          |> range(start: _start_time, stop: _stop_time)
          |> filter(fn: (r) => r["_measurement"] == "temperature")
          |> filter(fn: (r) => r["location"] == "building_a")
      initial_history: 24h
      lag_time: 5m
      overlap: 2m
      batch_duration: 1h
    polling:
      frequency: 5m
    redis:
      stream: "measurements.sensors.temperature"
      tags:
        location: "building_a"
        data_provider: "InfluxDB"
```

## Energy- and Market-Related Services
#### ENTSO-E Transparency Platform - Day-Ahead Market Prices
**Interface Type**: Source

**Type Name**: `data_crawler.sources.entsoe_da.ENTSOEDATransparency`

**Description**: The ENTSO-E day-ahead market prices source fetches the day-ahead market prices from the ENTSO-E. It 
requires [registration to the ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) and acquisition of an 
API token via the portal.

**Parameters**:
* `api_key`: The API key to access the ENTSO-E API.
* `day_ahead_prices`: The day-ahead market price configurations to be fetched. Each entry contains a dict with the 
  following attributes:
    * `country_code`: The country code of the market area to be queried. (e.g., `AT` for Austria)
    * `timezone`: The timezone of the market prices to be queried. This information is used to determine the beginning 
      and ending of the next day
    * `resolution`: The resolution of the market prices to be queried. Right now, `MIN_15`, `MIN_30`, and `MIN_60` are 
      supported.
