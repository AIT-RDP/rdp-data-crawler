import datetime
import logging
import time

import websocket
from typing import Any, Optional, Dict, Literal

import pydantic

import data_crawler.sinks.abc.abstract_sink as abstract_sink


class WebsocketParameters(abstract_sink.SinkParameters):
    url: str = pydantic.Field(
        description="The URL of the Websocket server to connect to",
        pattern=r"wss?://.*",
    )

    headers: Optional[dict[str, str]] = pydantic.Field(
        description="The headers to include in the Websocket request",
        default=None
    )

    # Extend this list as needed.
    # Relevant protocol may include: https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
    data_format: Literal['influx'] = pydantic.Field(
        description="The format of the data to be sent",
        default='influx',
    )


class WebsocketMetadata(abstract_sink.SinkMetadata):
    name: str = pydantic.Field(
        description="The name of this datapoint. For the influx data format, this will be the metric name.",
    )


def _convert_to_influx_line(data: Dict[str, Any], metadata: WebsocketMetadata) -> str:
    """
    Convert the data dictionary into an InfluxDB line protocol string.
    Uses metadata.name as the measurement name.
    Uses data.observation_time as the timestamp. If not present, uses the current time.

    This example treats all key-value pairs as fields.
    """
    if "observation_time" in data:
        # Convert to UTC
        dt_utc = data["observation_time"].astimezone(datetime.timezone.utc)
        del data["observation_time"]

        timestamp_sec: float = dt_utc.timestamp()
        timestamp = int(timestamp_sec * 1e9)
    else:
        # If there is no observation time, use the current time.
        timestamp = int(time.time() * 1e9)

    fields = []
    # There is also an influx library to do this, but the library is slow and not needed here.
    for key, value in data.items():
        if isinstance(value, float):
            fields.append(f"{key}={value}")
        elif isinstance(value, int):
            fields.append(f"{key}={value}i")
        elif isinstance(value, bool):
            fields.append(f"{key}={'true' if value else 'false'}")
        else:
            # Escape double quotes in string values if necessary.
            safe_val = str(value).replace('"', '\\"')
            fields.append(f'{key}="{safe_val}"')
    fields_part = ",".join(fields)

    # Replace spaces in the measurement name with underscores.
    # Just to make sure and not break the line protocol.
    metadata.name = metadata.name.replace(" ", "_")

    influx_line = f"{metadata.name} {fields_part} {timestamp}"
    return influx_line


class Websocket(abstract_sink.AbstractSinkAPI):
    """
    Implements a Websocket data sink.

    Data passed to this sink will be sent to a Websocket server in the specified format.
    """

    def __init__(self, websocket_config: WebsocketParameters):
        """
        Initialize the sink with a URL and optional headers.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        self.url = websocket_config.url
        self.headers = [f"{k}: {v}" for k, v in websocket_config.headers.items()] if websocket_config.headers else []
        self.ws: Optional[websocket.WebSocket] = None
        self.data_format = websocket_config.data_format
        self._connect()

    def _connect(self) -> None:
        """
        Attempt to create a WebSocket connection.
        """
        try:
            self.ws = websocket.create_connection(
                self.url,
                header=self.headers,
                ping_interval=20,
                ping_timeout=20
            )
            self._logger.info("Connection established.")
        except Exception as e:
            self._logger.error(f"Initial connection failed: {e}")
            self.ws = None

    def _reconnect(self) -> None:
        """
        Try to reconnect with exponential backoff.
        """
        backoff = 1
        while True:
            try:
                self.ws = websocket.create_connection(
                    self.url,
                    header=self.headers,
                    ping_interval=20,
                    ping_timeout=20
                )
                self._logger.info("Reconnected successfully.")
                break
            except Exception as e:
                self._logger.error(f"Reconnect failed ({e}), retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _is_connected(self) -> bool:
        """
        Check if the WebSocket connection is established.
        """
        return self.ws is not None and getattr(self.ws, 'connected', False)

    @classmethod
    def create(cls, sink_parameters: WebsocketParameters, **kwargs) -> abstract_sink.AbstractSinkAPI:
        return Websocket(sink_parameters)

    def insert_data(self, data: dict[str, Any], metadata: WebsocketMetadata) -> None:
        """
        Convert the given data to specified format, ensure that the WebSocket is
        connected (or attempt a reconnect if not), and send the data.
        """

        # Extract the observation time from the data if available.
        if "observation_time" in data:
            data["observation_time"] = datetime.datetime.fromisoformat(data["observation_time"])
        elif "timestamp" in data:
            data["observation_time"] = datetime.datetime.fromisoformat(data["timestamp"])
            del data["timestamp"]
        elif "time" in data:
            data["observation_time"] = datetime.datetime.fromisoformat(data["time"])
            del data["time"]

        if self.data_format == 'influx':
            formatted_data = _convert_to_influx_line(data, metadata)
        else:
            raise ValueError(f"Unsupported data format: {self.data_format}")

        if not self._is_connected():
            self._logger.debug("WebSocket not connected. Attempting to reconnect...")
            self._reconnect()

        try:
            self.ws.send(formatted_data)
            self._logger.debug(f"Sent: {formatted_data}")
        except Exception as e:
            self._logger.error(f"Error sending message: {e}")
            self._logger.debug("Attempting to reconnect and resend...")
            self._reconnect()
            try:
                self.ws.send(formatted_data)
                self._logger.debug(f"Resent: {formatted_data}")
            except Exception as ex:
                self._logger.error(f"Resend failed: {ex}")

    @staticmethod
    def parameter_model() -> type[abstract_sink.SinkParameters]:
        return WebsocketParameters

    @staticmethod
    def metadata_model() -> type[abstract_sink.SinkMetadata]:
        return WebsocketMetadata
