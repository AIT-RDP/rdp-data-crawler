import asyncio
import logging
import queue
import threading
from collections.abc import Generator
from typing import Optional
from rdp_mqtt.mqtt_client import MqttClient, MqttSettings
from rdp_mqtt.mqtt_parameters import MqttBaseParameters
from data_crawler.sources.abc.active_source_sync import AbstractSyncActiveSourceAPI, SourceParameters
from data_crawler.sources.abc.message import MessageData


class MqttSource(AbstractSyncActiveSourceAPI):
    """MQTT source that bridges async MQTT operations with the synchronous source API."""

    @classmethod
    def create(cls, source_parameters: SourceParameters, **kwargs) -> "AbstractSyncActiveSourceAPI":
        return cls(source_parameters, **kwargs)

    def __init__(self, source_parameters: SourceParameters, executor_name: str = "", **kwargs):
        super().__init__()

        self.executor_name = executor_name
        self.logger = logging.getLogger(f"{__name__}.{executor_name}")

        self.params = MqttBaseParameters.model_validate(source_parameters)

        mqtt_settings_dict = self.params.model_dump()

        mqtt_settings = MqttSettings.model_validate(mqtt_settings_dict)
        mqtt_settings.subscribe = True  # Always subscribe for sources

        self._mqtt_settings = mqtt_settings
        self._client: Optional[MqttClient] = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._async_thread: Optional[threading.Thread] = None
        self._sync_queue: queue.Queue[MessageData] = queue.Queue(maxsize=1000) # Prevent memory overflow
        self._shutdown_event = threading.Event()

    def _async_runner(self):
        """Run the MQTT client in the async thread."""
        self._async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._async_loop)

        async def run_mqtt_client():
            self._client = MqttClient(self._mqtt_settings)
            await self._client.setup()

            # When we close the loop in shutdown this can throw an exception
            # which will printed but ignored
            async for message in self._client.subscribe():
                if self._shutdown_event.is_set():
                    await self._client.shutdown()
                    break

                # If the consumer is too slow, we lose data
                try:
                    self._sync_queue.put_nowait(message)
                except queue.Full:
                    pass

        try:
            self._async_loop.run_until_complete(run_mqtt_client())
        except asyncio.CancelledError:
            pass  # Expected during shutdown
        except Exception as e:
            self.logger.error(f"MQTT client error: {e}")
        finally:
            try:
                if self._async_loop and not self._async_loop.is_closed():
                    self._async_loop.call_soon_threadsafe(self._async_loop.stop)
                    self._async_loop.run_forever()
                    self._async_loop.close()
            except Exception:
                pass

    def start(self):
        """Start the MQTT source by creating and starting the async thread."""
        self._async_thread = threading.Thread(target=self._async_runner)
        self._async_thread.start()

    def run(self) -> Generator[MessageData, None, None]:
        while not self._shutdown_event.is_set():
            try:
                yield self._sync_queue.get(timeout=1)
            except queue.Empty:
                continue

    def shutdown(self) -> None:
        """Shutdown the MQTT source and clean up resources."""
        self._shutdown_event.set()

        # Gracefully shut down the client if possible
        # We need to call shutdown ourselves if the async for message did not
        # have messages and shutdown was not called
        if self._client and self._async_loop and not self._async_loop.is_closed():
            try:
                fut = asyncio.run_coroutine_threadsafe(self._client.shutdown(), self._async_loop)
                fut.result(timeout=2)  # wait a bit for clean shutdown
            except Exception as e:
                self.logger.warning(f"Error during MQTT client shutdown: {e}")

        # Stop the event loop
        if self._async_loop and not self._async_loop.is_closed():
            try:
                self._async_loop.call_soon_threadsafe(self._async_loop.stop)
            except Exception as e:
                self.logger.debug(f"Error stopping loop: {e}")

        # Join the thread
        if self._async_thread and self._async_thread.is_alive():
            self._async_thread.join(timeout=3)

        # Close the loop after thread exits
        if self._async_loop and not self._async_loop.is_closed():
            try:
                self._async_loop.close()
            except Exception as e:
                self.logger.debug(f"Error closing loop: {e}")

        # Cleanup references
        self._async_loop = None
        self._async_thread = None
        self._client = None

    @staticmethod
    def parameter_model():
        return MqttBaseParameters
