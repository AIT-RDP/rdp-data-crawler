"""
Implements the interface to the Danish KNMI weather services

The module requires the numerics extras since it has to parse large grid and measurement data files. Documentation can
be found at the following locations:
 * Data point description: https://english.knmidata.nl/open-data/actuele10mindataknmistations
 * API Docs: https://tyk-cdn.dataplatform.knmi.nl/open-data/index.html
"""
import datetime
import itertools
from typing import Dict, Generator, Optional, Iterable
import io
import logging

import numpy as np
import pandas as pd

try:
    # Optional dependencies that come with the 'numerics' extras
    import xarray as xr
except ModuleNotFoundError:
    import warnings

    warnings.warn("xarray is not found. Most likely the optional 'numerics' dependencies are missing.")
    raise

import data_crawler.sources.abc.abstract_source as abstract_source
import data_crawler.sources.abc.http_cache as http_cache


class WeatherStationsKNMI(http_cache.SyncHTTPMixin, abstract_source.AbstractMultiMessageSourceAPI):
    """Queries the KNMI for the weather station measurements in the Netherlands"""

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        Initializes the KNMI API but does not trigger any query

        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purposes
        :param kwargs: Any extra arguments that will be sent to the super class
        """
        super(WeatherStationsKNMI, self).__init__(source_parameters=source_parameters, **kwargs)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": source_parameters["api_key"]}
        self.dataset_name = "Actuele10mindataKNMIstations"
        self.dataset_version = "2"
        self._station_config = [
            {"id": st} if isinstance(st, str) else st
            for st in source_parameters['stations']
        ]

        initial_history = pd.to_timedelta(source_parameters.get("initial_history", "12h"))
        self._last_query_ts = datetime.datetime.now(tz=datetime.timezone.utc) - initial_history

        self._batch_size = int(source_parameters.get("batch_size", 1000))  # Mostly for testing purpose

    def __get_data(self, url, params=None):
        self._logger.debug(f"Query KNMI API endpoint: {url} with {params}")
        return self.session.get(url, headers=self.headers, params=params).json()

    def _list_updated_files(self, dataset_name: str, dataset_version: str, start_date: datetime.datetime,
                            end_date: datetime.datetime) -> list[str]:
        """Lists the files that were updated within the given time frame and returns the corresponding file names"""

        base_params = {
            "maxKeys": self._batch_size, "orderBy": "lastModified", "sorting": "asc",
            "begin": start_date.isoformat(), "end": end_date.isoformat()
        }
        url = f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files"

        batches = [self.__get_data(url, params=base_params)]
        while ("nextPageToken" in batches[-1] and
               batches[-1]["nextPageToken"] is not None and
               batches[-1]["nextPageToken"] != ""):
            batches.append(self.__get_data(url, params={**base_params, "nextPageToken": batches[-1]["nextPageToken"]}))

        file_records = itertools.chain(*[batch["files"] for batch in batches])

        # Filter all timestamps that are smaller than the start_data because the API includes both ends.
        def _is_new(rec) -> bool:
            ts = datetime.datetime.fromisoformat(rec["lastModified"]).astimezone(datetime.timezone.utc)
            return ts > start_date

        file_records = filter(_is_new, file_records)
        return [rec["filename"] for rec in file_records]

    def _generate_raw_data_blobs(self, dataset_name: str, dataset_version: str, start_date: datetime.datetime,
                                 end_date: datetime.datetime) -> Generator[bytes, None, None]:
        """Generator function that yields one content file after another"""

        # get the name of the latest files first
        updates = self._list_updated_files(self.dataset_name, self.dataset_version, start_date, end_date)
        for filename in updates:
            # get the file and load it into an object
            file_url_response = self.get_file_url(self.dataset_name, self.dataset_version, filename)
            raw_file_content = self.session.get(file_url_response["temporaryDownloadUrl"], stream=True)
            yield raw_file_content.content

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )

    def fetch_data_bundle(self, raw_data: Optional[Iterable[bytes]] = None) -> Generator:
        """
        Fetches the weather station data and translates it into a common Redis-ready nomenclature

        :param raw_data: The raw measurement files for testing purpose. It is not advised to use the parameter
            productively.
        :return: The dictionary of forecasts following the common nomenclature. See the AbstractSourceAPI class for more
            information on the expected output format.
        """
        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        if raw_data is None:
            raw_data = self._generate_raw_data_blobs(self.dataset_name, self.dataset_version, self._last_query_ts,
                                                     ts_now)

        measurement_collection = []
        for hdf_data in raw_data:
            measurements_array = xr.open_dataset(io.BytesIO(hdf_data))  # now an xarray object
            measurements = measurements_array.to_dataframe()  # now a pandas dataframe
            # The measurements frame already comes with a MultiIndex with level station/time. The columns correspond to
            # the individual data fields. Hence, the frames holding a single instance of time can be directly
            # concatenated
            measurements = self._filter_stations(measurements)
            measurement_collection.append(measurements)

        if len(measurement_collection) > 0:
            # Join all time steps into a common frame for more efficient processing
            measurements_combined = pd.concat(measurement_collection, axis="index")
            # put the variables into a proper format
            measurements_combined = self._beautify_variables(measurements_combined)
            # Split the bunch of data into individual messages and add auxiliary information
            messages = self._convert_to_messages(measurements_combined)

            # return The formatted message data
            yield from messages

        self._last_query_ts = ts_now  # Don't use the measurement time as updates may arrive later.

    def _filter_stations(self, measurement_batch: pd.DataFrame) -> pd.DataFrame:
        """Slices the configured stations based on the station id in the first level of the MultiIndex-Index"""
        registered_stations = set(cnf["id"] for cnf in self._station_config)
        batch_filter = measurement_batch.index.get_level_values(0).isin(registered_stations)
        filtered_batch = measurement_batch.loc[batch_filter]

        return filtered_batch

    @staticmethod
    def _beautify_variables(dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Renames all the variables that are already implemented in the dataset. Keeps all the others with the original name.

        """

        # Add the index information as columns
        dataframe["station"] = dataframe.index.get_level_values(0)
        dataframe["time"] = dataframe.index.get_level_values(1)

        dataframe["location"] = dataframe["stationname"].map(lambda x: 'NL-' + x.replace(' ', '-'))

        # Some columns seem to be encoded as binary strings. No idea why.
        dataframe[["za", "nhc"]] = dataframe[["za", "nhc"]].map(lambda x: np.nan if x == b'' else x)

        ft_to_m = 0.3048
        octa_to_percentage = 1. / 8.
        m_to_km = 1. / 1000.

        dataframe[["h", "h1", "h2", "h3", "hc", "hc1", "hc2", "hc3"]] *= ft_to_m
        dataframe[["n", "n1", "n2", "n3", "nc", "nc1", "nc2", "nc3", "nhc"]] *= octa_to_percentage
        dataframe["vv"] *= m_to_km
        dataframe["D1H"] *= 100. / 60.  # minutes per one-hour moving average
        dataframe[["dr", "pr"]] *= 100. / (10 * 60.)  # seconds per 10-minutes period
        dataframe[["Q1H", "Q24H"]] *= 1e4 / 3600.  # J/(cm^2) to Wh/m²
        dataframe["ss"] *= 100. / 10.  # % from min per 10 minutes interval

        # convert time from UTC to timezone aware timestamps
        dataframe["time"] = dataframe["time"].dt.tz_localize('UTC')
        dataframe["time"] = dataframe["time"].map(lambda x: x.isoformat())

        # Mapping to the AIT RDP scheme. A more detailed documentation can be found at
        # https://english.knmidata.nl/open-data/actuele10mindataknmistations
        variable_names = {
            "time": "observation_time",  # "ISO TS"),
            "lat": "latitude",  # "deg"),
            "lon": "longitude",  # "deg"),
            "height": "altitude",  # "m"),
            "stationname": "station_name",  # "string"),
            "location": "location",  # "string"),
            "station": "device_id",  # "string"),
            "D1H": "rainfall_time_fraction_1h",  # "%"),
            "dr": "precipitation_time_fraction",  # "%"),
            "dd": "wind_direction_10m",  # "°"),
            "dn": "wind_direction_10m_min",  # "°"),
            "dx": "wind_direction_10m_max",  # "°"),
            "dsd": "wind_direction_10m_std",  # "°"),
            "ff": "wind_speed_10m",  # "m/s"),
            "ffs": "wind_speed_10m_sensor",  # "m/s"),
            "fsd": "wind_speed_10m_std",  # "m/s"),
            "gff": "wind_speed_gust_10m",  # "m/s"),
            "gffs": "wind_speed_gust_10m_sensor",  # "m/s"),
            "fx": "wind_speed_gust_10m_full_average",  # "m/s"), values without discontinuation filter
            "fxs": "wind_speed_gust_10m_sensor_full_average",  # "m/s"), values without discontinuation filter
            "h": "cloud_base",  # "m"), # multiply ft_to_m
            "h1": "cloud_base_low",  # "m"), # multiply ft_to_m
            "h2": "cloud_base_medium",  # "m"), # multiply ft_to_m
            "h3": "cloud_base_high",  # "m"), # multiply ft_to_m
            "hc": "cloud_base_ceilometer",  # "m"), # multiply ft_to_m
            "hc1": "cloud_base_low_ceilometer",  # "m"), # multiply ft_to_m
            "hc2": "cloud_base_medium_ceilometer",  # "m"), # multiply ft_to_m
            "hc3": "cloud_base_high_ceilometer",  # "m"), # multiply ft_to_m
            "n": "cloud_area_fraction",  # "%"),# multiply octa_to_percentage
            "n1": "cloud_area_fraction_low",  # "%"),# multiply octa_to_percentage
            "n2": "cloud_area_fraction_medium",  # "%"),# multiply octa_to_percentage
            "n3": "cloud_area_fraction_high",  # "%"),# multiply octa_to_percentage
            "nc": "cloud_area_fraction_ceilometer",  # "%"),# multiply octa_to_percentage
            "nc1": "cloud_area_fraction_low_ceilometer",  # "%"),# multiply octa_to_percentage
            "nc2": "cloud_area_fraction_medium_ceilometer",  # "%"),# multiply octa_to_percentage
            "nc3": "cloud_area_fraction_high_ceilometer",  # "%"),# multiply octa_to_percentage
            "nhc": "cloud_area_fraction_low_medium_ceilometer",  # "%"),# multiply octa_to_percentage
            "p0": "air_pressure",  # "hPa"),
            "pp": "air_pressure_at_sea_level",  # "hPa"),
            "ps": "air_pressure_at_sensor_level",  # "hPa"),
            "pg": "precipitation_rate_pws",  # "mm/h"),
            "rg": "precipitation_rate",  # "mm/h"),
            "pr": "precipitation_time_fraction_pws",  # "%" from "sec/10min"),
            "Q1H": "global_horizontal_irradiation_1h_sum",  # Wh/m² from "J/(cm^2)"),
            "Q24H": "global_horizontal_irradiation_24h_sum",  # Wh/m² from "J/(cm^2)"),
            "qg": "global_horizontal_irradiation",  # "W/(m^2)"),
            "qgn": "global_horizontal_irradiation_min",  # "W/(m^2)"),
            "qgx": "global_horizontal_irradiation_max",  # "W/(m^2)"),
            "R1H": "rainfall_total_1h",  # "mm"),
            "R6H": "rainfall_total_6h",  # "mm"),
            "R12H": "rainfall_total_12h",  # "mm"),
            "R24H": "rainfall_total_24h",  # "mm"),
            "rh": "relative_humidity_2m",  # %
            "rh10": "relative_humidity_2m_10min_avg",  # "%"),
            "Sav1H": "wind_speed_10m_1h_avg",  # "m/s"),
            "Sax1H": "wind_speed_10m_1h_max",  # "m/s"),
            "Sax3H": "wind_speed_10m_3h_max",  # "m/s"),
            "Sax6H": "wind_speed_10m_6h_max",  # "m/s"),
            "sq": "squall_indicator",  # "code wmo table 4680"),
            "ss": "sunshine_fraction",  # % from "min per 10 min interval,
            "Sx1H": "wind_speed_gust_10m_1h_max",  # "m/s"),
            "Sx3H": "wind_speed_gust_10m_3h_max",  # "m/s"),
            "Sx6H": "wind_speed_gust_10m_6h_max",  # "m/s"),
            "t10": "air_temperature",  # "°C" ), ## some station don't have either t10 or ta
            "ta": "air_temperature_2m",  # "°C" ), ## actually 1.5 meters, but whatever
            "tb": "wet_bulb_temperature_2m",  # "°C"),
            "tb1": "soil_temperature_5cm",  # "°C"),
            "Tb1n6": "soil_temperature_5cm_6h_min",  # "°C"),
            "Tb1x6": "soil_temperature_5cm_6h_max",  # "°C"),
            "tb2": "soil_temperature_10cm",  # "°C"),
            "Tb2n6": "soil_temperature_10cm_6h_min",  # "°C"),
            "Tb2x6": "soil_temperature_10cm_6h_max",  # "°C"),
            "tb3": "soil_temperature_20cm",  # "°C"),
            "tb4": "soil_temperature_50cm",  # "°C"),
            "tb5": "soil_temperature_100cm",  # "°C"),
            "td": "dew_point_temperature_2m",  # "°C"),
            "td10": "dew_point_temperature_2m_10min_avg",  # "°C"),
            "tg": "grass_temperature_10cm_10min_avg",  # "°C"),
            "tgn": "grass_temperature_10cm_10min_min",  # "°C"),
            "Tgn6": "grass_temperature_6h_min",  # "°C"),
            "Tgn12": "grass_temperature_12h_min",  # "°C"),
            "Tgn14": "grass_temperature_14h_min",  # "°C"),
            "tn": "air_temperature_2m_10min_min",  # "°C"),
            "tx": "air_temperature_2m_10min_max",  # "°C"),
            "Tn12": "air_temperature_2m_12h_min",  # "°C"),
            "Tn14": "air_temperature_2m_14h_min",  # "°C"),
            "Tn6": "air_temperature_2m_6h_min",  # "°C"),
            "Tx6": "air_temperature_2m_6h_max",  # "°C"),
            "Tx12": "air_temperature_2m_12h_max",  # "°C"),
            "Tx24": "air_temperature_2m_24h_max",  # "°C"),
            "vv": "visibility",  # "km"), ## multiply to km
            "W10": "past_weather_indicator",  # "code wmo table 4680"),
            "W10-10": "past_weather_indicator_10min",  # "code wmo table 4680"),
            "ww": "wawa_weather_code",  # "code wmo table 4680"),
            "ww-10": "wawa_weather_code_10min",  # "code wmo table 4680"),
            "za": "background_luminance",  # "cd/(m^2)"),
            "zm": "meteorological_optical_range",  # "m")
        }
        # "tsd"         :      ("siam_ambient_temperature_10min_avg_std_dev"           ,   "°C"),
        # "rh"          :      ("relative_humidity_1p5m_1min_avg"                      ,   "%"),
        # "pwc"         :      ("corrected_precipitation_type_10min_max"               ,   "code knmi handboek waarnemingen"),
        # "qnh"         :      ("qnh_1min_avg"                                         ,   "hpa"),

        # make a subselection of the dataframe
        dataframe = dataframe[variable_names.keys()]

        dataframe = dataframe.rename(columns=variable_names)

        return dataframe

    def _convert_to_messages(self, measurements_combined: pd.DataFrame) -> Generator[dict, None, None]:
        """
        Converts the station/time-indexed frame into a station-based message format.

        It is assumed that the columns are already transformed to the destination nomenclature.
        """

        stations = list(set(measurements_combined.index.get_level_values(0)))
        stations.sort()  # Mostly to ease testing
        for station in stations:
            station_data = measurements_combined.xs(station, level=0, axis="index")
            message = self._convert_to_message(station_data)

            # Override fields fixed by tags
            tags = self._get_station_config(station).get("tags", {})
            message = {**message, **tags}

            yield message

    @staticmethod
    def _convert_to_message(message_data: pd.DataFrame):
        """Converts the single message from the data frame to a dict structure"""
        meta_values = ["latitude", "longitude", "altitude", "location", "device_id"]
        message_in = message_data.to_dict(orient="list")
        message_out = {}
        for msg_key, msg_val in message_in.items():
            if msg_key in meta_values:
                assert len(msg_val) >= 1, "At least one message element must be given"
                assert all(v == msg_val[0] for v in msg_val), "All meta-fields must be equal"

                message_out[msg_key] = msg_val[0]  # Unpack the meta-field
            else:
                message_out[msg_key] = msg_val

        return message_out

    def _get_station_config(self, station_id: str) -> dict:
        """Returns the station specific configuration for the particular station"""

        cnf_candidates = list(filter(lambda x: x["id"] == station_id, self._station_config))
        if len(cnf_candidates) != 1:
            raise KeyError(f"Expect exactly one station entry for ID {station_id}, but {len(cnf_candidates)} found.")

        return cnf_candidates[0]
