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


class FroniusInverterPowerFlowRealtimeData(abstract_source.AbstractMultiMessageSourceAPI):
    """
    Implements the Fronius SolarAPI endpoint delivering  real-time power flow data

    The endpoint returns a reduced set of power flow and energy data from all devices of the system. Hence, it can be
    frequently any synchronously queried.
    """

    def __init__(self, source_parameters, executor_name, **kwargs):
        """
        :param source_parameters: The source parameters according to the configuration
        :param executor_name: The name of the executor for debugging purpose
        :param kwargs: Any extra arguments that will be sent to the super class
        """

        super().__init__(source_parameters=source_parameters, executor_name=executor_name, **kwargs)

        self._address = source_parameters["address"]
        self._correct_device_time = bool(source_parameters.get("correct device time", False))
        self._device_tags = source_parameters.get("device tags", {})  # dict of device specific tags to append

    def fetch_data_bundle(self, raw_data: Optional[dict] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Fetches the real-time response and returns one message per inverter

        :param raw_data: A raw data response to test the decoding functionality.
        :return: A generator that yields the individual messages
        """

        ts_now = datetime.datetime.now(tz=datetime.timezone.utc)
        if raw_data is None:
            raw_data = self._fetch_raw_data()
        _raise_for_fronius_status(raw_data)

        observation_time = raw_data["Head"]["Timestamp"]
        base_message = {
            "observation_time_device": observation_time,
            "observation_time": ts_now.isoformat() if self._correct_device_time else observation_time,
        }

        for inv_name, inv_data in raw_data["Body"]["Data"]["Inverters"].items():
            decoded_message = self._decode_single_inverter(inv_name, inv_data, base_message)
            yield decoded_message

    def _decode_single_inverter(self, inv_name: str, inv_data: dict, base_message: dict) -> dict:
        """Decodes the single inverter stanza and returns the result"""

        decoded_message = {
            "device_id": inv_name,
            "device_type": _fronius_device_types.get(inv_data["DT"], "Unknown Device"),
            "E_P_exp": inv_data["E_Total"],  # [Wh]
            "E_P_exp_day": inv_data["E_Day"],  # [Wh]
            "E_P_exp_year": inv_data["E_Year"],  # [Wh]
            "active_power_generation": inv_data.get("P", 0.0) / 1e3,  # Deprecated, [kW]
            "P_AC_tot": inv_data.get("P", 0.0),  # [W]
        }

        decoded_message.update(base_message)
        decoded_message.update(self._device_tags.get(inv_name, {}))

        return decoded_message

    def _fetch_raw_data(self) -> dict:
        """Queries the current raw data from the inverter"""

        _real_time_guard.wait_until_safe(self._address)  # Don't forget to be patient.
        resp = requests.get(f"http://{self._address}/solar_api/v1/GetPowerFlowRealtimeData.fcgi")
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

        self._fetch_ahead = pd.to_timedelta(source_parameters.get("fetch ahead", "10min"))

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

        ts_latest = ts_now + self._fetch_ahead if len(raw_data["Body"]["Data"]) > 0 else self._last_query_ts
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
            "device_type": _fronius_device_types.get(inv_data["DeviceType"], "Unknown Device"),
            "observation_time_device": [ts.isoformat() for ts in observation_time_device],
            "observation_time": [ts.isoformat() for ts in observation_time],
            "observation_time_span": [float(chan_data["TimeSpanInSec"]["Values"][ts]) for ts in ref_point_offset]
        }
        ret.update(self._device_tags.get(device_id, {}))

        for dp_name in self._data_points:
            if dp_name not in chan_data:
                self._logger.warning(f"The requested data point '{dp_name}' couldn't be fetched and will be ignored.")
                continue

            time_series = self._extract_time_series(chan_data[dp_name], ref_point_offset, dp_name)
            ret[self._parameter_mapping[dp_name]] = time_series

        return ret

    def _extract_time_series(self, dp_data: dict, offset_axis: List[str], dp_name: str) -> list:
        """Extracts the time series according to the offset axis and returns the result"""

        unavailable_points = set(offset_axis).difference(dp_data["Values"].keys())
        if len(unavailable_points) > 0:
            self._logger.warning(f"Some values of series {dp_name} on offsets {list(unavailable_points)} are not "
                                 "delivered and will be filled with None values.")

        excess_points = set(dp_data["Values"].keys()).difference(offset_axis)
        if len(excess_points) > 0:
            self._logger.warning(f"Time series of {dp_name} has intermediate data points ({list(excess_points)}) that "
                                 f"will be dropped")

        ret = [dp_data["Values"].get(ts, None) for ts in offset_axis]
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
        ts_now += self._fetch_ahead
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


_fronius_device_types = {  # Maps the type id to the appropriate device name
    67: "Fronius Primo 15.0-1 208-240",
    68: "Fronius Primo 12.5-1 208-240",
    69: "Fronius Primo 11.4-1 208-240",
    70: "Fronius Primo 10.0-1 208-240",
    71: "Fronius Symo 15.0-3 208",
    72: "Fronius Eco 27.0-3-S",
    73: "Fronius Eco 25.0-3-S",
    75: "Fronius Primo 6.0-1",
    76: "Fronius Primo 5.0-1",
    77: "Fronius Primo 4.6-1",
    78: "Fronius Primo 4.0-1",
    79: "Fronius Primo 3.6-1",
    80: "Fronius Primo 3.5-1",
    81: "Fronius Primo 3.0-1",
    82: "Fronius Symo Hybrid 4.0-3-S",
    83: "Fronius Symo Hybrid 3.0-3-S",
    84: "Fronius IG Plus 120 V-1",
    85: "Fronius Primo 3.8-1 208-240",
    86: "Fronius Primo 5.0-1 208-240",
    87: "Fronius Primo 6.0-1 208-240",
    88: "Fronius Primo 7.6-1 208-240",
    89: "Fronius Symo 24.0-3 USA Dummy",
    90: "Fronius Symo 24.0-3 480",
    91: "Fronius Symo 22.7-3 480",
    92: "Fronius Symo 20.0-3 480",
    93: "Fronius Symo 17.5-3 480",
    94: "Fronius Symo 15.0-3 480",
    95: "Fronius Symo 12.5-3 480",
    96: "Fronius Symo 10.0-3 480",
    97: "Fronius Symo 12.0-3 208-240",
    98: "Fronius Symo 10.0-3 208-240",
    99: "Fronius Symo Hybrid 5.0-3-S",
    100: "Fronius Primo 8.2-1 Dummy",
    101: "Fronius Primo 8.2-1 208-240",
    102: "Fronius Primo 8.2-1",
    103: "Fronius Agilo TL 360.0-3",
    104: "Fronius Agilo TL 460.0-3",
    105: "Fronius Symo 7.0-3-M",
    106: "Fronius Galvo 3.1-1 208-240",
    107: "Fronius Galvo 2.5-1 208-240",
    108: "Fronius Galvo 2.0-1 208-240",
    109: "Fronius Galvo 1.5-1 208-240",
    110: "Fronius Symo 6.0-3-M",
    111: "Fronius Symo 4.5-3-M",
    112: "Fronius Symo 3.7-3-M",
    113: "Fronius Symo 3.0-3-M",
    114: "Fronius Symo 17.5-3-M",
    115: "Fronius Symo 15.0-3-M",
    116: "Fronius Agilo 75.0-3 Outdoor",
    117: "Fronius Agilo 100.0-3 Outdoor",
    118: "Fronius IG Plus 55 V-1",
    119: "Fronius IG Plus 55 V-2",
    120: "Fronius Symo 20.0-3 Dummy",
    121: "Fronius Symo 20.0-3-M",
    122: "Fronius Symo 5.0-3-M",
    123: "Fronius Symo 8.2-3-M",
    124: "Fronius Symo 6.7-3-M",
    125: "Fronius Symo 5.5-3-M",
    126: "Fronius Symo 4.5-3-S",
    127: "Fronius Symo 3.7-3-S",
    128: "Fronius IG Plus 60 V-2",
    129: "Fronius IG Plus 60 V-1",
    130: "SPR 8001F-3 EU",
    131: "Fronius IG Plus 25 V-1",
    132: "Fronius IG Plus 100 V-3",
    133: "Fronius Agilo 100.0-3",
    134: "SPR 3001F-1 EU",
    135: "Fronius IG Plus V/A 10.0-3 Delta",
    136: "Fronius IG 50",
    137: "Fronius IG Plus 30 V-1",
    138: "SPR-11401f-1 UNI",
    139: "SPR-12001f-3 WYE277",
    140: "SPR-11401f-3 Delta",
    141: "SPR-10001f-1 UNI",
    142: "SPR-7501f-1 UNI",
    143: "SPR-6501f-1 UNI",
    144: "SPR-3801f-1 UNI",
    145: "SPR-3301f-1 UNI",
    146: "SPR 12001F-3 EU",
    147: "SPR 10001F-3 EU",
    148: "SPR 8001F-2 EU",
    149: "SPR 6501F-2 EU",
    150: "SPR 4001F-1 EU",
    151: "SPR 3501F-1 EU",
    152: "Fronius CL 60.0 WYE277 Dummy",
    153: "Fronius CL 55.5 Delta Dummy",
    154: "Fronius CL 60.0 Dummy",
    155: "Fronius IG Plus V 12.0-3 Dummy",
    156: "Fronius IG Plus V 7.5-1 Dummy",
    157: "Fronius IG Plus V 3.8-1 Dummy",
    158: "Fronius IG Plus 150 V-3 Dummy",
    159: "Fronius IG Plus 100 V-2 Dummy",
    160: "Fronius IG Plus 50 V-1 Dummy",
    161: "Fronius IG Plus V/A 12.0-3 WYE",
    162: "Fronius IG Plus V/A 11.4-3 Delta",
    163: "Fronius IG Plus V/A 11.4-1 UNI",
    164: "Fronius IG Plus V/A 10.0-1 UNI",
    165: "Fronius IG Plus V/A 7.5-1 UNI",
    166: "Fronius IG Plus V/A 6.0-1 UNI",
    167: "Fronius IG Plus V/A 5.0-1 UNI",
    168: "Fronius IG Plus V/A 3.8-1 UNI",
    169: "Fronius IG Plus V/A 3.0-1 UNI",
    170: "Fronius IG Plus 150 V-3",
    171: "Fronius IG Plus 120 V-3",
    172: "Fronius IG Plus 100 V-2",
    173: "Fronius IG Plus 100 V-1",
    174: "Fronius IG Plus 70 V-2",
    175: "Fronius IG Plus 70 V-1",
    176: "Fronius IG Plus 50 V-1",
    177: "Fronius IG Plus 35 V-1",
    178: "SPR 11400f-3 208/240",
    179: "SPR 12000f-277",
    180: "SPR 10000f",
    181: "SPR 10000F EU",
    182: "Fronius CL 33.3 Delta",
    183: "Fronius CL 44.4 Delta",
    184: "Fronius CL 55.5 Delta",
    185: "Fronius CL 36.0 WYE277",
    186: "Fronius CL 48.0 WYE277",
    187: "Fronius CL 60.0 WYE277",
    188: "Fronius CL 36.0",
    189: "Fronius CL 48.0",
    190: "Fronius IG TL 3.0",
    191: "Fronius IG TL 4.0",
    192: "Fronius IG TL 5.0",
    193: "Fronius IG TL 3.6",
    194: "Fronius IG TL Dummy",
    195: "Fronius IG TL 4.6",
    196: "SPR 12000F EU",
    197: "SPR 8000F EU",
    198: "SPR 6500F EU",
    199: "SPR 4000F EU",
    200: "SPR 3300F EU",
    201: "Fronius CL 60.0",
    202: "SPR 12000f",
    203: "SPR 8000f",
    204: "SPR 6500f",
    205: "SPR 4000f",
    206: "SPR 3300f",
    207: "Fronius IG Plus 12.0-3 WYE277",
    208: "Fronius IG Plus 50",
    209: "Fronius IG Plus 100",
    210: "Fronius IG Plus 100",
    211: "Fronius IG Plus 150",
    212: "Fronius IG Plus 35",
    213: "Fronius IG Plus 70",
    214: "Fronius IG Plus 70",
    215: "Fronius IG Plus 120",
    216: "Fronius IG Plus 3.0-1 UNI",
    217: "Fronius IG Plus 3.8-1 UNI",
    218: "Fronius IG Plus 5.0-1 UNI",
    219: "Fronius IG Plus 6.0-1 UNI",
    220: "Fronius IG Plus 7.5-1 UNI",
    221: "Fronius IG Plus 10.0-1 UNI",
    222: "Fronius IG Plus 11.4-1 UNI",
    223: "Fronius IG Plus 11.4-3 Delta",
    224: "Fronius Galvo 3.0-1",
    225: "Fronius Galvo 2.5-1",
    226: "Fronius Galvo 2.0-1",
    227: "Fronius IG 4500-LV",
    228: "Fronius Galvo 1.5-1",
    229: "Fronius IG 2500-LV",
    230: "Fronius Agilo 75.0-3",
    231: "Fronius Agilo 100.0-3 Dummy",
    232: "Fronius Symo 10.0-3-M",
    233: "Fronius Symo 12.5-3-M",
    234: "Fronius IG 5100",
    235: "Fronius IG 4000",
    236: "Fronius Symo 8.2-3-M Dummy",
    237: "Fronius IG 3000",
    238: "Fronius IG 2000",
    239: "Fronius Galvo 3.1-1 Dummy",
    240: "Fronius IG Plus 80 V-3",
    241: "Fronius IG Plus 60 V-3",
    242: "Fronius IG Plus 55 V-3",
    243: "Fronius IG 60 ADV",
    244: "Fronius IG 500",
    245: "Fronius IG 400",
    246: "Fronius IG 300",
    247: "Fronius Symo 3.0-3-S",
    248: "Fronius Galvo 3.1-1",
    249: "Fronius IG 60 HV",
    250: "Fronius IG 40",
    251: "Fronius IG 30 Dummy",
    252: "Fronius IG 30",
    253: "Fronius IG 20",
    254: "Fronius IG 15",
}
