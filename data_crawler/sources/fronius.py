"""
Implements the Fronius APIs to query device information
"""
import datetime
import itertools
import logging
import re
import urllib
from typing import Dict, Any, Optional, List, Generator

import pandas as pd
import requests

import data_crawler.access.guards as guards
import data_crawler.access.jsonpath as jx
import data_crawler.sources.abc.abstract_source as abstract_source

# The access guard of all real-time queries
_real_time_guard = guards.SequentialTimingGuard(4.0)


class FroniusInverterRealtimeData(abstract_source.AbstractSourceAPI):
    """
    Implements the device-level API to query the real-time data of one particular device

    In contrast to other APIs, no caching is implemented as it does not make any sense to cache real-time data.
    """

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super().__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        self._address = source_parameters["address"]
        self._device_id = int(source_parameters.get("device", 1))
        self._correct_device_time = bool(source_parameters.get("correct device time", False))

        self._extractors = self._compile_extractors()

    @staticmethod
    def _compile_extractors() -> List[jx.PathExtractor]:
        """Returns the list of available JSON extractors"""

        parameter_mapping = {
            "I_AC_tot": "IAC",  # [A]
            "I_DC_tot": "IDC",  # [A]
            "U_AC": "UAC",  # [V]
            "U_DC": "UDC",  # [V]
            "Frequency": "FAC",  # [Hz]
            "E_P_exp": "TOTAL_ENERGY",  # "Wh"
            "E_P_exp_day": "DAY_ENERGY",  # "Wh"
            "E_P_exp_year": "YEAR_ENERGY",  # "Wh"
        }
        extractors = [
            jx.PathExtractor(dst, f"Body.Data.{src}.Value", is_list=False, drop_missing=True)
            for dst, src in parameter_mapping.items()
        ]

        extractors += [
            jx.OptionalPathExtractor("P_AC_tot", "Body.Data", "PAC.Value", is_list=False, default_value=0.0),
            # Deprecated variable to establish compatibility. Will be removed in future revisions:
            jx.OptionalPathExtractor("active_power_generation", "Body.Data", "PAC.Value", is_list=False,
                                     default_value=0.0,
                                     dst_format=lambda x: x * 1e-3),

            jx.DatetimePathExtractor("observation_time", "Head.Timestamp", is_list=False),
            jx.PathExtractor("error_code", "Body.Data.DeviceStatus.ErrorCode", is_list=False, drop_missing=True),
            jx.PathExtractor("status_code", "Body.Data.DeviceStatus.StatusCode", is_list=False, drop_missing=True),
            jx.PathExtractor("device_id", "Head.RequestArguments.DeviceId", is_list=False, drop_missing=True),
        ]

        return extractors

    def fetch_data(self, raw_data: Optional[dict] = None) -> Dict[str, Any]:
        """
        Retrieves the data from the given logging device and returns the result.

        :param raw_data: The raw data for testing purpose
        :return: The compiled message from the inverters
        """

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)

        if raw_data is None:
            raw_data = self._fetch_next_raw_result()

        _raise_for_fronius_status(raw_data)
        decoded_message = self._decode_raw_message(raw_data)

        decoded_message["P_DC_tot"] = decoded_message.get("I_DC_tot", 0.0) * decoded_message.get("U_DC", 0.0)
        decoded_message["observation_time_device"] = decoded_message["observation_time"]

        if self._correct_device_time:
            decoded_message["observation_time"] = ts_now.isoformat()

        return decoded_message

    def _decode_raw_message(self, raw_data: dict) -> Dict[str, Any]:
        """Decodes the raw message into the common message format"""

        redis_forecast = dict(itertools.chain(*[ext.extract_information(raw_data).items() for ext in self._extractors]))
        return redis_forecast

    def _fetch_next_raw_result(self) -> dict:
        """Queries the next raw result from the inverter"""

        _real_time_guard.wait_until_safe(self._address)  # Don't forget to be patient.
        resp = requests.get(f"http://{self._address}/solar_api/v1/GetInverterRealtimeData.cgi", params={
            "Scope": "Device", "DeviceId": self._device_id, "DataCollection": "CommonInverterData"
        })
        resp.raise_for_status()
        return resp.json()


# The access guard of all history queries
_history_guard = guards.SequentialTimingGuard(120.0)


class FroniusSystemArchiveData(abstract_source.AbstractMultiMessageSourceAPI):
    """
    Implements the device-level API to query historic values from a fronius data logger.

    In contrast to other APIs, no caching es implemented since the historic values are updated quite frequently (~5min)
    """

    _parameter_mapping = {
        "EnergyReal_WAC_Sum_Produced": "E_P_exp_interval",  # [Wh]
        "EnergyReal_WAC_Sum_Consumed": "E_P_imp_interval",  # [Wh]
        "Current_DC_String_1": "I_DC_S1",  # [1A]
        "Current_DC_String_2": "I_DC_S2",  # [1A]
        "Voltage_DC_String_1": "U_DC_S1",  # [1V]
        "Voltage_DC_String_2": "U_DC_S2",  # [1V]
        "Temperature_Powerstage": "device_temperature_1",  # [deg C]
        "Voltage_AC_Phase_1": "U_L1N",  # [1V]
        "Voltage_AC_Phase_2": "U_L2N",  # [1V]
        "Voltage_AC_Phase_3": "U_L3N",  # [1V]
        "Current_AC_Phase_1": "I_L1",  # [1A]
        "Current_AC_Phase_2": "I_L2",  # [1A]
        "Current_AC_Phase_3": "I_L3",  # [1A]
        "PowerReal_PAC_Sum": "P_AC_avg",  # [1W]
    }

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super().__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{executor_name}")

        self._address = source_parameters["address"]
        self._correct_device_time = bool(source_parameters.get("correct device time", False))

        self._data_points = set(source_parameters.get("data points", list(self._parameter_mapping.keys())))
        unknown_data_points = self._data_points.difference(self._parameter_mapping.keys())
        if len(unknown_data_points) > 0:
            raise ValueError(f"Unknown data points {unknown_data_points}, only the following are supported: "
                             f"{list(self._parameter_mapping.keys())}")

        self._device_tags = source_parameters.get("device tags", {})  # dict of device specific tags to append

        initial_history = pd.to_timedelta(source_parameters.get("initial history", "48h"))
        self._last_query_ts = datetime.datetime.now(tz=datetime.timezone.utc) - initial_history

    def fetch_data_bundle(self, raw_data: Optional[dict] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Fetches the archive response starting from the last successful time stamp

        :param raw_data: A raw data response to test the decoding functionality.
        :return: A generator that yields the individual messages
        """

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        if raw_data is None:
            raw_data = self._fetch_raw_result(ts_now)
        _raise_for_fronius_status(raw_data)

        if self._correct_device_time:
            query_time = datetime.datetime.fromisoformat(raw_data["Head"]["Timestamp"])
            time_offset = ts_now - query_time
        else:
            time_offset = datetime.timedelta(seconds=0.0)

        ts_latest = ts_now
        for inv_name, inv_data in raw_data["Body"]["Data"].items():
            message_data = self._decode_raw_inverter_data(inv_name, inv_data, time_offset)
            message_data = self._extend_message_data(message_data)

            ts_latest = min(ts_latest, datetime.datetime.fromisoformat(message_data["observation_time_device"][-1]))
            yield message_data

        self._last_query_ts = ts_latest

    @staticmethod
    def _extend_message_data(message_data: dict) -> dict:
        """Extends the message data by some pre-calculated quantities"""

        if "I_DC_S1" in message_data and "U_DC_S1" in message_data:
            message_data["P_DC_S1"] = [u * i for u, i in zip(message_data["U_DC_S1"], message_data["I_DC_S1"])]
        if "I_DC_S2" in message_data and "U_DC_S2" in message_data:
            message_data["P_DC_S2"] = [u * i for u, i in zip(message_data["U_DC_S2"], message_data["I_DC_S2"])]

        return message_data

    def _decode_raw_inverter_data(self, inv_name: str, inv_data: dict, time_offset: datetime.timedelta) -> dict:
        """Decodes the inverter-specific section and returns the message format"""

        device_id = self._extract_inverter_id(inv_name)
        start_time = datetime.datetime.fromisoformat(inv_data["Start"])
        chan_data = inv_data["Data"]

        ref_point_offset = sorted(chan_data["TimeSpanInSec"]["Values"].keys(), key=int)
        observation_time_device = [start_time + datetime.timedelta(seconds=int(ts)) for ts in ref_point_offset]
        observation_time = observation_time_device
        if self._correct_device_time:
            observation_time = [ts + time_offset for ts in observation_time_device]

        ret = {
            "device_id": device_id,
            "observation_time_device": [ts.isoformat() for ts in observation_time_device],
            "observation_time": [ts.isoformat() for ts in observation_time],
            "observation_time_span": [float(chan_data["TimeSpanInSec"]["Values"][ts]) for ts in ref_point_offset]
        }
        ret.update(self._device_tags.get(device_id, {}))

        for dp_name in self._data_points:
            if dp_name not in chan_data:
                self._logger.warning(f"The requested data point '{dp_name}' couldn't be fetched and will be ignored.")
                continue

            dp_time = sorted(chan_data[dp_name]["Values"].keys(), key=int)
            if dp_time != ref_point_offset:
                raise ValueError(f"Offset index of {dp_name}, {dp_time} differs from the reference {ref_point_offset}")

            ret[self._parameter_mapping[dp_name]] = [chan_data[dp_name]["Values"][ts] for ts in ref_point_offset]

        return ret

    @staticmethod
    def _extract_inverter_id(inv_name):
        """Extracts the numeric inverter id from the given name or returns the name, if it is not possible"""

        match = re.search("^inverter/(\d+)$", inv_name)
        if match:
            return str(match.group(1))
        else:
            return inv_name

    def _fetch_raw_result(self, ts_now) -> dict:
        """Returns the raw result from the current period"""

        # Jump to the next complete minute
        ts_now = datetime.datetime(ts_now.year, ts_now.month, ts_now.day, ts_now.hour, ts_now.minute, 0,
                                   tzinfo=ts_now.tzinfo) + datetime.timedelta(minutes=1)
        # Floor the seconds
        ts_start = self._last_query_ts
        ts_start = datetime.datetime(ts_start.year, ts_start.month, ts_start.day, ts_start.hour, ts_start.minute, 0,
                                     tzinfo=ts_start.tzinfo)

        # The Archive API freaks out if we URL-encode characters like ':' and '+'. Hence we have to build the URL
        # manually. :-(
        channels = list(self._data_points) + ["TimeSpanInSec"]
        url = f"http://{self._address}/solar_api/v1/GetArchiveData.cgi?Scope=System&HumanReadable=False&" \
              f"StartDate={ts_start.isoformat(timespec='seconds')}&EndDate={ts_now.isoformat(timespec='seconds')}&" + \
              "&".join([f"Channel={urllib.parse.quote(chn)}" for chn in channels])

        _history_guard.wait_until_safe(self._address)
        resp = requests.get(url)
        self._logger.debug(f"Tried to fetch history from {self._last_query_ts.isoformat()} to {ts_now.isoformat()}: "
                           f"{resp.request.url} - got {resp.status_code} {resp.reason}")
        resp.raise_for_status()
        return resp.json()


def _raise_for_fronius_status(raw_data):
    """Parses the fronius status description and returns the result"""
    if "Head" not in raw_data or "Status" not in raw_data["Head"]:
        raise KeyError(f"The device response does not contain any status information: {raw_data}")
    status_code = raw_data["Head"]["Status"]["Code"]
    if status_code != 0:
        raise ValueError(f"The Fronius device returned error {status_code}: {raw_data['Head']['Status']['Reason']} "
                         f"- {raw_data['Head']['Status']['UserMessage']}")
