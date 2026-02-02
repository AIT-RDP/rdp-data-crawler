"""
Implements some type checking functions that can be used across the entire package

The module adds some helper functions and types to be used with the pydantic framework.
"""

import typing_extensions
import datetime

import pydantic


def _transform_timedelta(value: datetime.timedelta | str) -> datetime.timedelta | str:
    """
    Transforms a possibly string-based timedelta into a proper timedelta object

    The function tries to import pandas in order to leverage its parsing capabilities. In case pandas is not
    installed, the value will be returned as-is to be parsed by successive validators.
    """

    if isinstance(value, str):
        try:
            import pandas as pd

            return pd.to_timedelta(value).to_pytimedelta()
        except ImportError:
            return value
    else:
        return value


TimedeltaType = typing_extensions.Annotated[
    datetime.timedelta,
    pydantic.BeforeValidator(_transform_timedelta)
]


def to_timezone(value: datetime.tzinfo | str) -> datetime.tzinfo:
    """
    Transforms a possibly string-based timezone into a proper tzinfo object

    The function tries to import pytz in order to leverage its timezone capabilities. In case pytz is not
    installed, the function will raise an error.
    """

    if isinstance(value, str):
        try:
            import pytz

            return pytz.timezone(value)
        except ImportError:
            raise ValueError("The 'pytz' package is required to parse string-based timezones.")

    else:
        return value

