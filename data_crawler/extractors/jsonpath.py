"""
Implements helper classes that use jsonpath expressions to extract some information
"""

from typing import Dict, Any

import jsonpath_rw
import pandas as pd


class PathExtractor:
    """Helper class to define an extraction rule transforming the parsed response"""

    def __init__(self, target_key: str, src_path: str, is_list=True, dst_format=None):
        """
        Initializes the extractor

        :param target_key: The destination key of all extracted values
        :param src_path: The JSON Path expression extracting the information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        :param dst_format: A callable that transforms each value to a destination format. In case it is None, no
            transformation will be applied.
        """

        self._target_key = target_key
        self._src_expression = jsonpath_rw.parse(src_path)
        self._is_list = is_list
        self._dst_format = dst_format

    def _extract_raw_results(self, raw_data: dict) -> list:
        """
        Internal hook to extract the information without any postprocessing steps

        :param raw_data: The pile of input data to fetch the information from
        :return: A possibly empty list of extracted values
        """

        return [r.value for r in self._src_expression.find(raw_data)]

    def extract_information(self, raw_data: dict) -> Dict[str, Any]:
        """
        Extracts the specified information and returns it in the destination dictionary form

        :param raw_data: The input structure as nested dictionaries
        :return: The output structure as transformed dictionary
        """

        result = self._extract_raw_results(raw_data)

        if self._dst_format is not None:
            result = list(map(self._dst_format, result))

        if not self._is_list:
            if len(result) != 1:
                raise KeyError(f"The path expression {self._src_expression} does not yield exactly one element "
                               f"but {len(result)} ones")
            result = result[0]

        return {self._target_key: result}


class DatetimePathExtractor(PathExtractor):
    """A path extractor that converts each timestamp value to the ISO 8610 format"""

    def __init__(self, target_key: str, src_path: str, is_list=True):
        """
        Initializes the extractor

        :param target_key: The destination key of all extracted values
        :param src_path: The JSON Path expression extracting the information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        """
        super(DatetimePathExtractor, self).__init__(target_key, src_path, is_list, self._to_iso)

    @staticmethod
    def _to_iso(x) -> str:
        """transforms the string representation to a common ISO 8610 format"""

        return pd.to_datetime(x).isoformat()


class OptionalPathExtractor(PathExtractor):
    """JSON Path extractor that allow to handle optional sub-paths"""

    def __init__(self, target_key: str, base_path: str, src_path: str, is_list=True, dst_format=None,
                 default_value=None):
        """
        Initializes the extractor

        In case src_path is found multiple times within base path, multiple values will be returned

        :param target_key: The destination key of all extracted values
        :param base_path: The JSON base path that defines the presence of each key
        :param src_path: The JSON sub path within the base path extracting the actual information
        :param is_list: A Flag whether the result must be a list of individual values. In case no list is expected, the
            path must point to a single value.
        :param dst_format: A callable that transforms each value to a destination format. In case it is None, no
            transformation will be applied.
        :param default_value: The default value to set in case the src_path is not present in the base_path
        """

        super(OptionalPathExtractor, self).__init__(target_key, src_path, is_list=is_list, dst_format=dst_format)

        self._base_expression = jsonpath_rw.parse(base_path)
        self._default_value = default_value

    def _extract_raw_results(self, raw_data: dict) -> list:
        """Parses the base paths and tries to find the sub-paths within"""

        bases = [r.value for r in self._base_expression.find(raw_data)]
        result = []
        for base in bases:
            sub_res = super(OptionalPathExtractor, self)._extract_raw_results(base)
            if len(sub_res) <= 0:
                sub_res = [self._default_value]
            result += sub_res
        return result
