"""
Data source to receive requests from a Teltonika RMS router which which acts as ModbusTCP client to query devices.
"""

import dataclasses
import datetime as dt
import logging
import multiprocessing as mp
import queue
import secrets
import threading
import typing

import uvicorn
from typing import Annotated, Generator

import fastapi
import fastapi.security
import pydantic

import data_crawler.sources.abc.active_source_sync as active_source_sync
from data_crawler.sources.abc import message as msg


class ModbusModel(pydantic.BaseModel):
    """
    Data from one read of Modbus registers
    """

    timestamp: dt.datetime = pydantic.Field(description="Timestamp of the read of the registers")
    name: str = pydantic.Field(
        description="Name of the request "
                    "(Teltonika WebUI: Services / Modbus / Modbus TCP Client / Modbus TCP Devices "
                    "/ column Action / Edit / Request Configuration / column Name)"
    )
    server_name: str = pydantic.Field(
        description="Name of the Modbus TCP device "
                    "(Teltonika WebUI: Services / Modbus / Modbus TCP Client / Modbus TCP Devices / column Name)"
    )
    data: list[float] = pydantic.Field(description="Array with the values of the read registers")


class PayloadModel(pydantic.BaseModel):
    """
    Payload sent from the router
    """

    modbus: ModbusModel


class DataTranslationDatabase(pydantic.BaseModel):
    """
    Configuration how the data from a request should be inserted into the sink such that it compatible with the table
    ` measurements` of the database
    """

    scaling: float = pydantic.Field(default=1.0, description="Scaling factor for the value")
    db_name: str = pydantic.Field(description="Name of the datapoint in the database")
    db_device_id: str = pydantic.Field(description="Device ID of the datapoint in the database")
    db_location_code: str = pydantic.Field(description="Location code of the datapoint in the database")
    db_data_provider: str = pydantic.Field(description="Data provider for the datapoint in the database")
    db_unit: str = pydantic.Field(default="", description="Unit for the datapoint in the database")
    db_metadata: dict = pydantic.Field(default={}, description="Metadata of the datapoint in the database")


class DataTranslation(DataTranslationDatabase):
    """
    Translation from data in a request to data which will be inserted into the database table `measurements`

    E.g. a request is received with the payload:
    {
        "modbus": {
            "timestamp": "2024-07-22T12:00:00Z",
            "server_name": "HAL 9000 - Sleeping pot Dr. David Bowman",
            "name": "Voltage_LL",
            "data": [10.0, 20.0, 30.0],
        }
    }

    Then a DataTranslation entry:
    DataTranslation(
        request_server_name="HAL 9000",
        request_name="Voltage_LL",
        request_data_index=1,
        db_name="U_L2L3",
        db_device_id="Sleeping pot Dr. David Bowman",
        db_location_code="Jupyter",
        db_data_provider="HAL 9000",
        db_unit="V",
    )

    would map to the second data entry (`20.0`), and a message with the following payload will be sent to the sink:
    {
        "obs_time": "2024-07-22T12:00:00Z",
        "value": 20.0,
        "name="U_L2L3",
        "device_id": "Sleeping pot Dr. David Bowman",
        "location_code": "Jupyter",
        "data_provider": "HAL 9000",
        "unit": "V",
        "metadata": {},
    }

    """

    request_server_name: str = pydantic.Field(description="Name of the Modbus TCP device (see ModbusModel.server_name)")
    request_name: str = pydantic.Field(description="Name of the request (see ModbusModel.name)")
    request_data_index: int = pydantic.Field(
        description="Index of the value in the data array of the request (see ModbusModel.data)"
    )


@dataclasses.dataclass(frozen=True, slots=True)
class RequestMetadata:
    """
    Metadata contained in a request (this is used for efficient matching of translations inside the source)
    """

    request_server_name: str
    request_name: str
    request_data_index: int


class UsernamePasswordParameters(pydantic.BaseModel):
    """
    Username and password for BasicAuth
    """

    username: bytes
    password: bytes


class FastAPIParameters(pydantic.BaseModel):
    """
    Parameters for the instantiation of the FastAPI class
    (see https://fastapi.tiangolo.com/reference/fastapi/#fastapi.FastAPI)
    """

    title: str = "AIT RDP - Teltonika Modbus Source"
    version: str = "0.1.0"
    root_path: str = pydantic.Field(
        default="",
        description="""Subpath under which the application will be serverd.
E.g. https://xkcd.com/api/v1 -> `root_path="/api/v1"`
If you are using Traefik strip the subpath by adding the following labels:
# strip /api/v1 from the request URL: /api/v1/systems -> /systems
- "traefik.http.middlewares.strip_api_v1.stripprefix.prefixes=/api/v1"
- "traefik.http.middlewares.strip_api_v1.stripprefix.forceSlash=false"
- "traefik.http.routers.rest_api.middlewares=strip_api_v1"
""",
    )


class TeltonikaModbusParameters(active_source_sync.SourceParameters):
    """
    Parameters of the source
    """
    
    host: str = pydantic.Field(
        default="0.0.0.0",
        description="Host of the server (use `0.0.0.0` if requests from everywhere should be received and `127.0.0.1` "
                    "if requests only from the local machine should be received",
    )
    port: int = pydantic.Field(default=8000, description="Port under which the endpoints should be served")
    block_time_ms: int = pydantic.Field(
        description="The client will make a blocking read and waits the selected time (ms) for new messages. "
                    "Afterwards it will check if a shutdown request was sent or if it should make another blocking "
                    "method call to read messages.",
        default=100,
    )
    data_translation: list[DataTranslation] = pydantic.Field(
        description="Translation records how the data from requests is translated so that it can be inserted into "
                    "the database"
    )
    basic_auth: pydantic.conlist(UsernamePasswordParameters, min_length=1) = pydantic.Field(
        description="Usernames and password valid to be used for BasicAuth"
    )
    fastapi: FastAPIParameters = pydantic.Field(
        default=FastAPIParameters(),
        description="Parameters for the instantiation of the FastAPI main class "
                    "(see https://fastapi.tiangolo.com/reference/fastapi/#fastapi.FastAPI)",
    )


class TeltonikaModbus(active_source_sync.AbstractSyncActiveSourceAPI):
    """
    Implements the TeltonikaModbus data source
    (Device --(ModbusTCP)-> Teltonika router --(REST)-> TeltonikaModbus source)
    """

    def __init__(self, config: TeltonikaModbusParameters):
        """Initializes the object"""

        super().__init__()

        self._logger_name = f"{__name__}.{self.__class__.__name__}"
        self._logger = logging.getLogger(self._logger_name)

        self._config: TeltonikaModbusParameters = config

        #: Message queue for the communication with the FastAPI server which is served in its own process
        self._data_queue: mp.Queue = mp.Queue()
        self._shutdown_request: bool = False

        #: Process in which the FastAPI server is served
        self._server_process: mp.Process | None = None

        self._last_wakeup: dt.datetime | None = None
        self._last_cycle_complete: dt.datetime | None = None
        self._activity_status_lock: threading.Lock = threading.Lock()

    @classmethod
    def create(cls, source_parameters: TeltonikaModbusParameters, **kwargs) -> "TeltonikaModbus":
        """
        Factory function that creates a new source

        :param source_parameters: The configuration of the TeltonikaModbus
        :return: The newly constructed data source instance
        """

        return cls(source_parameters)

    @staticmethod
    def parameter_model() -> type[TeltonikaModbusParameters]:
        return TeltonikaModbusParameters

    def _init_data_translation(self) -> dict[RequestMetadata, DataTranslationDatabase]:
        """
        Create an efficient dictionary for getting a translation data entry from the metadata of a request
        """

        return {
            RequestMetadata(
                request_server_name=d.request_server_name,
                request_name=d.request_name,
                request_data_index=d.request_data_index,
            ): d
            for d in self._config.data_translation
        }

    def start(self) -> None:
        """
        Starts the operation of the data source
        """

        self._server_process = self._create_server_process()
        self._server_process.start()

    def run(self) -> Generator[msg.MessageData, None, None]:
        """
        Executes the source and return messages as they arrive.

        The generator exits latest after the blocking time `block_time_ms`
        (plus the time it needs to process incoming requests when requests arrive at the end of the blocking time)
        as soon as a shutdown request is issued.
        """

        while not self._shutdown_request:
            try:
                with self._activity_status_lock:
                    self._last_wakeup = dt.datetime.now(tz=dt.timezone.utc)

                while (
                    (not self._shutdown_request)
                    # wait the time `self._config.block_time_ms` for new entries in the queue,
                    # if no entries (requests) come in this time, a queue.empty exception gets raised
                    and (item := self._data_queue.get(timeout=self._config.block_time_ms / 1000))
                ):
                    yield msg.Message(payload=item)
            except queue.Empty:
                ...

            with self._activity_status_lock:
                self._last_cycle_complete = dt.datetime.now(tz=dt.timezone.utc)

        # uvicorn accepts SIGINT signals but Windows does not know them -> terminate the process instead
        self._server_process.terminate()
        # wait for the process to be terminated
        self._server_process.join()
        self._shutdown_request = False

    def shutdown(self) -> None:
        """
        Indicates that the run generator must stop
        """

        self._shutdown_request = True

    def stop(self) -> None:
        assert not self._server_process.is_alive()
        self._server_process = None

    def get_activity_status(self) -> active_source_sync.ActivityStatus:
        """
        Returns the current activity status of the source

        :return: The current status information on the source activity
        """

        with self._activity_status_lock:
            return active_source_sync.ActivityStatus(
                last_wakeup=self._last_wakeup,
                last_cycle_complete=self._last_cycle_complete,
                max_permitted_cycle_time=dt.timedelta(milliseconds=self._config.block_time_ms),
            )

    @staticmethod
    def _create_fastapi_app(
            data_queue: mp.Queue,
            data_translation: dict[RequestMetadata, DataTranslationDatabase],
            basic_auth: list[UsernamePasswordParameters],
            logger_name: str,
            fastapi_parameters: FastAPIParameters,
    ) -> fastapi.FastAPI:
        """
        Create a FastAPI app with BasicAuth with an endpoint `/data`
        """

        logger = logging.getLogger(logger_name)

        app: fastapi.FastAPI = fastapi.FastAPI(**fastapi_parameters.model_dump())
        security: fastapi.security.HTTPBasic() = fastapi.security.HTTPBasic()

        data_translation: dict[RequestMetadata, DataTranslationDatabase] = data_translation
        basic_auth: dict[bytes, UsernamePasswordParameters] = \
            {username_password.username: username_password for username_password in basic_auth}

        def get_current_username(
                credentials: Annotated[fastapi.security.HTTPBasicCredentials, fastapi.Depends(security)]
        ) -> str | None:
            current_username: bytes = credentials.username.encode("utf8")
            current_password: bytes = credentials.password.encode("utf8")

            is_correct_username = current_username in basic_auth

            correct_password: bytes = (
                basic_auth[current_username].password
                if is_correct_username
                # create a dummy password when the username is wrong to prevent timing attacks
                # (even when the username is wrong a password should be checked)
                else b"dummy_password"
            )
            is_correct_password = secrets.compare_digest(
                current_password, correct_password
            )

            if not (is_correct_username and is_correct_password):
                raise fastapi.HTTPException(
                    status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
                    detail="Incorrect username or password",
                    headers={"WWW-Authenticate": "Basic"},
                )
            return credentials.username

        def _translate_data(data: PayloadModel) -> list[dict]:
            """
            Translate the data from the request to data which can be inserted into the table `measurements`
            of the database
            """

            result: list[dict] = []

            for index, value in enumerate(data.modbus.data):

                requests_metadata = RequestMetadata(
                    request_server_name=data.modbus.server_name,
                    request_name=data.modbus.name,
                    request_data_index=index,
                )

                try:
                    data_translation_database = data_translation[requests_metadata]
                except KeyError:
                    logger.error(f"No translation for request metadata `{requests_metadata}` found")
                    continue

                result.append(dict(
                    obs_time=data.modbus.timestamp.isoformat(),
                    value=value,
                    name=data_translation_database.db_name,
                    device_id=data_translation_database.db_device_id,
                    location_code=data_translation_database.db_location_code,
                    data_provider=data_translation_database.db_data_provider,
                    unit=data_translation_database.db_unit,
                    metadata=data_translation_database.db_metadata,
                ))

            return result

        @app.post("/data")
        async def data_post(
            data: PayloadModel,
            _username: Annotated[str, fastapi.Depends(get_current_username)],
        ) -> None:
            for item in _translate_data(data):
                data_queue.put(item)

        # the Teltonika router sends the requests with wrong headers
        # ("content-type": "application/x-www-form-urlencoded")
        # replace the header with ("content-type": "application/json")
        @app.middleware("http")
        async def change_header(request: fastapi.Request, call_next):
            if request.scope["path"] == "/data":
                headers = dict(request.scope["headers"])

                # replace the header
                if b"content-type" in headers and headers[b"content-type"] == b"application/x-www-form-urlencoded":
                    headers[b"content-type"] = b"application/json"

                request.scope["headers"] = list(headers.items())

            return await call_next(request)
        return app

    @classmethod
    def _run_server(
            cls,
            host: str,
            port: int,
            data_queue: mp.Queue,
            data_translation: dict[RequestMetadata, DataTranslationDatabase],
            basic_auth: list[UsernamePasswordParameters],
            logger_name: str,
            fastapi_parameters: FastAPIParameters,
    ) -> typing.NoReturn:
        """
        Run the FastAPI server with univorn
        """

        uvicorn.run(
            app=cls._create_fastapi_app(
                data_queue=data_queue,
                data_translation=data_translation,
                basic_auth=basic_auth,
                logger_name=logger_name,
                fastapi_parameters=fastapi_parameters,
            ),
            host=host,
            port=port,
        )

    def _create_server_process(self) -> mp.Process:
        """
        Create a process for the FastAPI server
        """

        return mp.Process(
            target=self._run_server,
            kwargs=dict(
                host=self._config.host,
                port=self._config.port,
                data_queue=self._data_queue,
                data_translation=self._init_data_translation(),
                basic_auth=self._config.basic_auth,
                logger_name=self._logger_name,
                fastapi_parameters=self._config.fastapi,
            ),
        )
