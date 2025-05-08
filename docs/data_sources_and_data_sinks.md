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
* `endpoint`: The name of the endpoint to be used. Either `TAWES` or `climate`. The default is `climate`.
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
