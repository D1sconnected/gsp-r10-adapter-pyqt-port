from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging
import platform

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from google.protobuf.json_format import MessageToDict

from gsp_r10.config import BluetoothConfig
from gsp_r10.models import FEET_TO_METERS
from gsp_r10.protocol import (
    BATTERY_CHARACTERISTIC_UUID,
    CONTROL_POINT_CHARACTERISTIC_UUID,
    DEVICE_INTERFACE_NOTIFIER_UUID,
    DEVICE_INTERFACE_WRITER_UUID,
    DEVICE_INFO_SERVICE_UUID,
    FIRMWARE_CHARACTERISTIC_UUID,
    MODEL_CHARACTERISTIC_UUID,
    R10Protocol,
    SERIAL_NUMBER_CHARACTERISTIC_UUID,
    STATUS_CHARACTERISTIC_UUID,
)
from gsp_r10.proto import launch_monitor_pb2 as proto

LOGGER = logging.getLogger(__name__)
LINUX_INTERFACE_NOTIFY_CANDIDATES = [
    DEVICE_INTERFACE_NOTIFIER_UUID,
    "6A4E2810-667B-11E3-949A-0800200C9A66",
    "6A4E2811-667B-11E3-949A-0800200C9A66",
]


class R10Client:
    def __init__(self, config: BluetoothConfig) -> None:
        self.config = config
        self.ble_device: BLEDevice | None = None
        self.client: BleakClient | None = None
        self.protocol = R10Protocol(self._write_chunk, debug_logging=config.debug_logging)
        self.protocol.message_sent_callbacks.append(lambda message: self.protocol.log_proto("<<", message))
        self.protocol.message_received_callbacks.append(lambda message: self.protocol.log_proto(">>", message))
        self.protocol.state_callback = self._on_state
        self.protocol.metrics_callback = self._on_metrics
        self.protocol.error_callback = self._on_error
        self.protocol.tilt_callback = self._on_tilt_calibration
        self.on_metrics: Callable[[dict], Awaitable[None] | None] | None = None
        self.on_state: Callable[[str], Awaitable[None] | None] | None = None
        self.on_device_info: Callable[[dict], Awaitable[None] | None] | None = None
        self._battery: int | None = None
        self._seen_shot_ids: set[int] = set()
        self._ready_event = asyncio.Event()
        self._interface_notifier_uuid = DEVICE_INTERFACE_NOTIFIER_UUID

    async def connect(self) -> None:
        self.ble_device = await self._find_device()
        LOGGER.info("Connecting to %s: %s", self.ble_device.name, self.ble_device.address)
        self.client = BleakClient(self.ble_device, disconnected_callback=self._on_disconnect)
        await self.client.connect()
        LOGGER.info("Connected to Launch Monitor")
        await self.protocol.start()
        await self._setup_notifications()
        info = await self._read_device_info()
        if self.on_device_info is not None:
            await _await_if_needed(self.on_device_info(info))
        success = await self.protocol.perform_handshake()
        if not success:
            raise RuntimeError("R10 handshake did not complete")
        await self._initialize_device()

    async def disconnect(self) -> None:
        await self.protocol.stop()
        if self.client and self.client.is_connected:
            await self.client.disconnect()

    @staticmethod
    async def scan(timeout: float = 10.0) -> list[BLEDevice]:
        devices = await BleakScanner.discover(timeout=timeout, return_adv=False)
        devices.sort(key=lambda device: ((device.name or "").lower(), device.address))
        return devices

    async def _find_device(self) -> BLEDevice:
        if self.config.address:
            device = await BleakScanner.find_device_by_address(self.config.address, timeout=10.0)
            if device is None:
                raise RuntimeError(f"Could not find device at address {self.config.address}")
            return device

        def exact_match(device: BLEDevice, _: object) -> bool:
            return device.name == self.config.bluetooth_device_name

        device = await BleakScanner.find_device_by_filter(exact_match, timeout=10.0)
        if device is not None:
            return device

        devices = await self.scan(timeout=6.0)
        target_name = self.config.bluetooth_device_name.casefold()
        partial_matches = [
            candidate for candidate in devices if candidate.name and target_name in candidate.name.casefold()
        ]
        if len(partial_matches) == 1:
            LOGGER.warning(
                "Exact BLE name '%s' was not found; using partial match '%s' at %s",
                self.config.bluetooth_device_name,
                partial_matches[0].name,
                partial_matches[0].address,
            )
            return partial_matches[0]

        visible = [f"{candidate.name or '<no name>'} [{candidate.address}]" for candidate in devices]
        raise RuntimeError(
            f"Could not find BLE device named '{self.config.bluetooth_device_name}'. "
            "Pair it in Windows settings, put it in pairing mode, then retry. "
            f"Visible devices: {visible if visible else 'none'}"
        )

    async def _setup_notifications(self) -> None:
        assert self.client is not None
        LOGGER.debug("Subscribing to measurement service")
        await self.client.start_notify("6A4E3401-667B-11E3-949A-0800200C9A66", self._ignore_notification)
        await asyncio.sleep(0.2)
        LOGGER.debug("Subscribing to control service")
        await self.client.start_notify(CONTROL_POINT_CHARACTERISTIC_UUID, self._ignore_notification)
        await asyncio.sleep(0.2)
        LOGGER.debug("Subscribing to status service")
        await self.client.start_notify(STATUS_CHARACTERISTIC_UUID, self._status_notification)
        await asyncio.sleep(0.2)
        LOGGER.debug("Reading battery service")
        await self.client.start_notify(BATTERY_CHARACTERISTIC_UUID, self._battery_notification)
        await asyncio.sleep(0.2)
        LOGGER.debug("Setting up device interface service")
        await self._start_device_interface_notifications()

    async def _start_device_interface_notifications(self) -> None:
        assert self.client is not None
        candidates = [DEVICE_INTERFACE_NOTIFIER_UUID]
        if platform.system() == "Linux":
            candidates = LINUX_INTERFACE_NOTIFY_CANDIDATES

        last_error: Exception | None = None
        for candidate in candidates:
            for attempt in range(1, 4):
                try:
                    LOGGER.debug(
                        "Subscribing to Garmin interface notifier %s (attempt %s/3)",
                        candidate,
                        attempt,
                    )
                    await self.client.start_notify(candidate, self._device_interface_notification)
                    self._interface_notifier_uuid = candidate
                    LOGGER.info("Using Garmin interface notifier %s", candidate)
                    return
                except Exception as exc:
                    last_error = exc
                    LOGGER.warning(
                        "Failed to subscribe to Garmin interface notifier %s on attempt %s: %s",
                        candidate,
                        attempt,
                        exc,
                    )
                    await asyncio.sleep(0.5)
                    if not self.client.is_connected:
                        raise
            LOGGER.warning("Moving to next Garmin interface notifier candidate after %s", candidate)

        if last_error is not None:
            raise last_error
        raise RuntimeError("Could not subscribe to any Garmin interface notifier characteristic")

    async def _read_device_info(self) -> dict:
        assert self.client is not None
        LOGGER.debug("Getting device info service %s", DEVICE_INFO_SERVICE_UUID)
        serial = (await self.client.read_gatt_char(SERIAL_NUMBER_CHARACTERISTIC_UUID)).decode("ascii", errors="ignore")
        firmware = (await self.client.read_gatt_char(FIRMWARE_CHARACTERISTIC_UUID)).decode("ascii", errors="ignore")
        model = (await self.client.read_gatt_char(MODEL_CHARACTERISTIC_UUID)).decode("ascii", errors="ignore")
        return {
            "model": model,
            "firmware": firmware,
            "serial": serial,
            "battery": self._battery,
        }

    async def _initialize_device(self) -> None:
        wake = await self._request_with_retries("wakeUpRequest", self.protocol.wake_device, attempts=3, delay=1.0)
        if wake is None:
            raise RuntimeError("Failed to wake R10")

        await self._wait_for_ready(timeout=5.0)

        status = await self._request_with_retries("statusRequest", self.protocol.status_request, attempts=3, delay=1.0)
        tilt = await self._request_with_retries("tiltRequest", self.protocol.tilt_request, attempts=3, delay=1.0)
        subscribe = await self._request_with_retries("subscribeRequest", self.protocol.subscribe_to_alerts, attempts=3, delay=1.0)

        start_tilt = None
        if self.config.calibrate_tilt_on_connect:
            start_tilt = await self._request_with_retries(
                "startTiltCalRequest",
                self.protocol.start_tilt_calibration,
                attempts=3,
                delay=1.0,
            )

        shot_config = await self._request_with_retries(
            "shotConfigRequest",
            lambda: self.protocol.shot_config(
                temperature=self.config.temperature,
                humidity=self.config.humidity,
                altitude=self.config.altitude,
                air_density=self.config.air_density,
                tee_range=self.config.tee_distance_in_feet * FEET_TO_METERS,
            ),
            attempts=3,
            delay=1.0,
        )

        device_info = await self._read_device_info()
        LOGGER.info("Device Setup Complete:")
        LOGGER.info("   Model: %s", device_info["model"])
        LOGGER.info("   Firmware: %s", device_info["firmware"])
        LOGGER.info("   Bluetooth ID: %s", self.ble_device.address if self.ble_device else "unknown")
        LOGGER.info("   Battery: %s%%", self._battery if self._battery is not None else "unknown")
        if status is not None and status.service.HasField("status_response"):
            LOGGER.info("   Current State: %s", proto.State.StateType.Name(status.service.status_response.state.state))
        if tilt is not None and tilt.service.HasField("tilt_response"):
            LOGGER.info("   Tilt: %s", MessageToDict(tilt.service.tilt_response.tilt, preserving_proto_field_name=True))

        missing = []
        if status is None:
            missing.append("status")
        if tilt is None:
            missing.append("tilt")
        if subscribe is None:
            missing.append("subscribe")
        if self.config.calibrate_tilt_on_connect and start_tilt is None:
            missing.append("startTiltCal")
        if shot_config is None:
            missing.append("shotConfig")
        if missing:
            LOGGER.warning("Setup finished with missing responses: %s", ", ".join(missing))

    async def _request_with_retries(
        self,
        label: str,
        request_factory: Callable[[], Awaitable[object | None]],
        *,
        attempts: int,
        delay: float,
    ) -> object | None:
        for attempt in range(1, attempts + 1):
            response = await request_factory()
            if response is not None:
                return response
            if attempt < attempts:
                LOGGER.warning("%s timed out; retrying (%s/%s)", label, attempt, attempts)
                await asyncio.sleep(delay)
        LOGGER.warning("%s did not return a response after %s attempts", label, attempts)
        return None

    async def _wait_for_ready(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            LOGGER.warning("R10 did not report ready within %.1f seconds", timeout)
            return False

    async def _write_chunk(self, data: bytes) -> None:
        assert self.client is not None
        await self.client.write_gatt_char(DEVICE_INTERFACE_WRITER_UUID, data, response=True)

    def _ignore_notification(self, _: object, __: bytearray) -> None:
        return

    def _status_notification(self, _: object, data: bytearray) -> None:
        if len(data) >= 3:
            is_awake = data[1] == 0
            is_ready = data[2] == 0
            if is_ready:
                self._ready_event.set()
            else:
                self._ready_event.clear()
            LOGGER.debug("Status notification awake=%s ready=%s raw=%s", is_awake, is_ready, bytes(data).hex())

    def _battery_notification(self, _: object, data: bytearray) -> None:
        if data:
            self._battery = int(data[0])
            LOGGER.info("Battery Life Updated: %s%%", self._battery)

    def _device_interface_notification(self, _: object, data: bytearray) -> None:
        asyncio.create_task(self.protocol.enqueue_ble_chunk(bytes(data)))

    def _on_disconnect(self, _: BleakClient) -> None:
        LOGGER.error("Lost bluetooth connection")

    def _on_state(self, state: object) -> None:
        state_name = proto.State.StateType.Name(state.state)
        LOGGER.info("State changed: %s", state_name)
        if state.state == proto.State.WAITING:
            self._ready_event.set()
        if self.on_state is not None:
            result = self.on_state(state_name)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)

    def _on_metrics(self, metrics: object) -> None:
        shot_id = int(metrics.shot_id)
        if shot_id in self._seen_shot_ids:
            LOGGER.warning("Received duplicate shot data %s. Ignoring", shot_id)
            return
        self._seen_shot_ids.add(shot_id)
        payload = MessageToDict(metrics, preserving_proto_field_name=True)
        LOGGER.info("Shot %s metrics received", shot_id)
        if self.on_metrics is not None:
            result = self.on_metrics(payload)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)

    def _on_error(self, error: object) -> None:
        LOGGER.error("Device error: %s", MessageToDict(error, preserving_proto_field_name=True))

    def _on_tilt_calibration(self, _: object) -> None:
        LOGGER.info("Tilt calibration update received")


async def _await_if_needed(result: Awaitable[None] | None) -> None:
    if result is not None:
        await result
