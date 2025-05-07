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
  hours (default)

### met.no
    * `data_crawler.sources.yr_no.LocationForecast` Numerical weather prediction data for a particular location

### Geosphere Austria
    * `data_crawler.sources.zamg.MeasurementStationData`: Live and historic measurements
    * `data_crawler.sources.zamg.NumericalWeatherPredictionData`: Numerical weather prediction data, both point 
      predictions and ensemble forecasts.

### KNMI
    * `data_crawler.sources.knmi.WeatherStationsKNMI`: Weather station data

## Generic protocols and interfaces
  * `data_crawler.sources.modbus.ModbusTCP`: Modbus TCP source
  * `data_crawler.sinks.modbus.ModbusTCP`; Modbus TCP sink
  * `data_crawler.sources.opc_ua.OPCUA`: OPC UA source
  * `data_crawler.sinks.opc_ua.OPCUA` OPC UA sink
  * `data_crawler.sinks.redis.RedisStream`: Redis stream sink (default)
  * `data_crawler.sources.teltonika_modbus.TeltonikaModbus`: REST interface to receive Modbus data via Teltonica devices

## Energy- and market-related services:

### ENTSO-E
    * `data_crawler.sources.entsoe_da.ENTSOEDATransparency`: Day-ahead market prices from ENTSO-E

## Device-specific interfaces
### Fronius
    * `data_crawler.sources.fronius.FroniusInverterRealtimeData`: Device-level real-time data from Fronius inverters
    * `data_crawler.sources.fronius.FroniusInverterPowerFlowRealtimeData`: Real-time power-flow data of all devices 
      connected to the data logger
    * `data_crawler.sources.fronius.FroniusSystemArchiveData`: Device-level API to query historic values and detailed
      information from Fronius inverters