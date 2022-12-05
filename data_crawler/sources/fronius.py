"""
Implements the Fronius APIs to query device information
"""
import datetime
import itertools
from typing import Dict, Any, Optional, List

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

        self._raise_for_fronius_status(raw_data)
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

    @staticmethod
    def _raise_for_fronius_status(raw_data):
        """Parses the fronius status description and returns the result"""
        if "Head" not in raw_data or "Status" not in raw_data["Head"]:
            raise KeyError(f"The device response does not contain any status information: {raw_data}")
        status_code = raw_data["Head"]["Status"]["Code"]
        if status_code != 0:
            raise ValueError(f"The Fronius device returned error {status_code}: {raw_data['Head']['Status']['Reason']} "
                             f"- {raw_data['Head']['Status']['UserMessage']}")

    def _fetch_next_raw_result(self) -> dict:
        """Queries the next raw result from the inverter"""

        _real_time_guard.wait_until_safe(self._address)  # Don't forget to be patient.
        resp = requests.get(f"http://{self._address}/solar_api/v1/GetInverterRealtimeData.cgi", params={
            "Scope": "Device", "DeviceId": self._device_id, "DataCollection": "CommonInverterData"
        })
        resp.raise_for_status()
        return resp.json()
