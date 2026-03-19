from __future__ import annotations

from dataclasses import dataclass
import asyncio
import logging
from typing import Callable

from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message

from gsp_r10.proto import LaunchMonitor_pb2 as proto

LOGGER = logging.getLogger(__name__)


BATTERY_SERVICE_UUID = "0000180f-0000-1000-8000-00805f9b34fb"
BATTERY_CHARACTERISTIC_UUID = "00002a19-0000-1000-8000-00805f9b34fb"
DEVICE_INFO_SERVICE_UUID = "0000180a-0000-1000-8000-00805f9b34fb"
FIRMWARE_CHARACTERISTIC_UUID = "00002a28-0000-1000-8000-00805f9b34fb"
MODEL_CHARACTERISTIC_UUID = "00002a24-0000-1000-8000-00805f9b34fb"
SERIAL_NUMBER_CHARACTERISTIC_UUID = "00002a25-0000-1000-8000-00805f9b34fb"
DEVICE_INTERFACE_SERVICE_UUID = "6A4E2800-667B-11E3-949A-0800200C9A66"
DEVICE_INTERFACE_NOTIFIER_UUID = "6A4E2812-667B-11E3-949A-0800200C9A66"
DEVICE_INTERFACE_WRITER_UUID = "6A4E2822-667B-11E3-949A-0800200C9A66"
MEASUREMENT_SERVICE_UUID = "6A4E3400-667B-11E3-949A-0800200C9A66"
MEASUREMENT_CHARACTERISTIC_UUID = "6A4E3401-667B-11E3-949A-0800200C9A66"
CONTROL_POINT_CHARACTERISTIC_UUID = "6A4E3402-667B-11E3-949A-0800200C9A66"
STATUS_CHARACTERISTIC_UUID = "6A4E3403-667B-11E3-949A-0800200C9A66"
PROTO_PREFIX_LEN = 16


def to_hex(data: bytes | bytearray) -> str:
    return bytes(data).hex().upper()


def crc16(data: bytes) -> bytes:
    polynomial = 0xA001
    table: list[int] = []
    for i in range(256):
        value = 0
        temp = i
        for _ in range(8):
            if ((value ^ temp) & 0x0001) != 0:
                value = (value >> 1) ^ polynomial
            else:
                value >>= 1
            temp >>= 1
        table.append(value)

    crc = 0
    for b in data:
        crc = ((crc >> 8) ^ table[(crc ^ b) & 0xFF]) & 0xFFFF
    return crc.to_bytes(2, "little")


def cobs_encode(data: bytes) -> bytes:
    result = bytearray()
    distance_index = 0
    distance = 1

    for value in data:
        if value != 0 and distance < 255:
            result.append(value)
            distance += 1
        else:
            result.insert(distance_index, distance)
            distance_index = len(result)
            distance = 1

    if result and len(result) != 255:
        result.insert(distance_index, distance)

    return bytes(result)


def cobs_decode(data: bytes) -> bytes:
    data_array = bytes(data)
    result = bytearray()
    distance_index = 0

    while distance_index < len(data_array):
        distance = data_array[distance_index]
        if len(data_array) < distance_index + distance or distance < 1:
            return b""

        if distance > 1:
            result.extend(data_array[distance_index + 1 : distance_index + distance])

        distance_index += distance

        if distance < 0xFF and distance_index < len(data_array):
            result.append(0)

    return bytes(result)


@dataclass(slots=True)
class DeviceInfo:
    model: str
    firmware: str
    serial: str
    battery: int | None


class R10Protocol:
    def __init__(
        self,
        write_chunk: Callable[[bytes], asyncio.Future | asyncio.Task | object],
        *,
        debug_logging: bool = False,
    ) -> None:
        self._write_chunk = write_chunk
        self.debug_logging = debug_logging
        self.message_received_callbacks: list[Callable[[Message], None]] = []
        self.message_sent_callbacks: list[Callable[[Message], None]] = []
        self.state_callback: Callable[[object], None] | None = None
        self.metrics_callback: Callable[[object], None] | None = None
        self.error_callback: Callable[[object], None] | None = None
        self.tilt_callback: Callable[[object], None] | None = None

        self._reader_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._message_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._reader_task: asyncio.Task[None] | None = None
        self._processor_task: asyncio.Task[None] | None = None
        self._running = False
        self._header = 0x00
        self._handshake_complete = False
        self._handshake_event = asyncio.Event()
        self._proto_request_counter = 0
        self._pending_response: proto.WrapperProto | None = None
        self._proto_response_event = asyncio.Event()

    def _is_debug_enabled(self) -> bool:
        return self.debug_logging or LOGGER.isEnabledFor(logging.DEBUG)

    async def start(self) -> None:
        self._running = True
        self._reader_task = asyncio.create_task(self._reader_loop(), name="r10-reader")
        self._processor_task = asyncio.create_task(self._processor_loop(), name="r10-processor")

    async def stop(self) -> None:
        self._running = False
        tasks = [task for task in (self._reader_task, self._processor_task) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def enqueue_ble_chunk(self, data: bytes) -> None:
        if self._is_debug_enabled():
            LOGGER.debug("      -> %s (ble read)", to_hex(data))
        await self._reader_queue.put(data)

    async def perform_handshake(self) -> bool:
        if self._is_debug_enabled():
            LOGGER.debug("Starting handshake")
        self._handshake_complete = False
        self._handshake_event.clear()
        self._header = 0x00
        await self.send_bytes(bytes.fromhex("000000000000000000010000"))
        try:
            await asyncio.wait_for(self._handshake_event.wait(), timeout=10)
            return True
        except asyncio.TimeoutError:
            LOGGER.error("Handshake did not complete")
            return False

    async def send_bytes(self, payload: bytes) -> None:
        framed = bytes([self._header]) + payload
        if self._is_debug_enabled():
            LOGGER.debug("      <- %s (ble write)", to_hex(payload))
        result = self._write_chunk(framed)
        if asyncio.iscoroutine(result):
            await result

    async def write_message(self, payload: bytes) -> None:
        if self._is_debug_enabled():
            LOGGER.debug("<- %s (raw)", to_hex(payload))

        length = (2 + len(payload) + 2).to_bytes(2, "little")
        framed = length + payload
        full_frame = framed + crc16(framed)

        if self._is_debug_enabled():
            LOGGER.debug("  <- %s (framed)", to_hex(full_frame))

        encoded = b"\x00" + cobs_encode(full_frame) + b"\x00"
        if self._is_debug_enabled():
            LOGGER.debug("    <- %s (encoded)", to_hex(encoded))

        for start in range(0, len(encoded), 19):
            await self.send_bytes(encoded[start : start + 19])

    async def send_protobuf_request(self, wrapper: proto.WrapperProto) -> proto.WrapperProto | None:
        self._proto_response_event.clear()
        self._pending_response = None
        message_bytes = wrapper.SerializeToString()
        msg_len = len(message_bytes).to_bytes(4, "little")
        full_message = (
            bytes.fromhex("B313")
            + self._proto_request_counter.to_bytes(4, "little")
            + b"\x00\x00"
            + msg_len
            + msg_len
            + message_bytes
        )

        await self.write_message(full_message)
        for callback in self.message_sent_callbacks:
            callback(wrapper)

        try:
            await asyncio.wait_for(self._proto_response_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            LOGGER.error("Failed to get response for proto %s", self._proto_request_counter)
            return None

        self._proto_request_counter += 1
        return self._pending_response

    async def wake_device(self) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        wrapper.service.wake_up_request.SetInParent()
        return await self.send_protobuf_request(wrapper)

    async def status_request(self) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        wrapper.service.status_request.SetInParent()
        return await self.send_protobuf_request(wrapper)

    async def tilt_request(self) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        wrapper.service.tilt_request.SetInParent()
        return await self.send_protobuf_request(wrapper)

    async def subscribe_to_alerts(self) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        alert = wrapper.event.subscribe_request.alerts.add()
        alert.type = proto.AlertNotification.LAUNCH_MONITOR
        return await self.send_protobuf_request(wrapper)

    async def start_tilt_calibration(self) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        wrapper.service.start_tilt_cal_request.SetInParent()
        return await self.send_protobuf_request(wrapper)

    async def shot_config(
        self,
        *,
        temperature: float,
        humidity: float,
        altitude: float,
        air_density: float,
        tee_range: float,
    ) -> proto.WrapperProto | None:
        wrapper = proto.WrapperProto()
        request = wrapper.service.shot_config_request
        request.temperature = temperature
        request.humidity = humidity
        request.altitude = altitude
        request.air_density = air_density
        request.tee_range = tee_range
        return await self.send_protobuf_request(wrapper)

    def log_proto(self, direction: str, message: Message) -> None:
        LOGGER.info("%s %s", direction, MessageToJson(message, preserving_proto_field_name=False).replace("\n", " "))

    async def _reader_loop(self) -> None:
        current_message = bytearray()
        while self._running:
            msg = await self._reader_queue.get()
            header = msg[0]
            payload = msg[1:]

            if header == 0 or not self._handshake_complete:
                await self._continue_handshake(payload)
                continue

            read_complete = False
            if payload and payload[-1] == 0x00:
                read_complete = True
                payload = payload[:-1]
            if payload and payload[0] == 0x00:
                current_message.clear()
                payload = payload[1:]

            current_message.extend(payload)

            if read_complete and current_message:
                if self._is_debug_enabled():
                    LOGGER.debug("  -> %s (encoded)", to_hex(current_message))
                decoded = cobs_decode(bytes(current_message))
                if self._is_debug_enabled():
                    LOGGER.debug("-> %s (decoded)", to_hex(decoded))
                await self._message_queue.put(decoded)
                current_message.clear()

    async def _continue_handshake(self, payload: bytes) -> None:
        payload_hex = to_hex(payload)
        if self._is_debug_enabled():
            LOGGER.debug("Handshake candidate: %s", payload_hex)
        if payload_hex.startswith("010000000000000000010000"):
            self._header = payload[12]
            await self.send_bytes(bytes.fromhex("00"))
            self._handshake_complete = True
            self._handshake_event.set()
            if self._is_debug_enabled():
                LOGGER.debug("Handshake complete with header %02X", self._header)

    async def _processor_loop(self) -> None:
        while self._running:
            frame = await self._message_queue.get()
            await self._process_message(frame)

    async def _process_message(self, frame: bytes) -> None:
        if crc16(frame[:-2]) != frame[-2:]:
            LOGGER.warning("CRC error for frame %s", to_hex(frame))

        msg = frame[2:-2]
        hex_msg = to_hex(msg)
        ack_body = bytearray(b"\x00")

        if hex_msg.startswith("A013"):
            pass
        elif hex_msg.startswith("BA13"):
            pass
        elif hex_msg.startswith("B413"):
            counter = int.from_bytes(msg[2:4], "little")
            ack_body.extend(msg[2:4])
            ack_body.extend(bytes.fromhex("00000000000000"))
            if counter == self._proto_request_counter:
                wrapper = proto.WrapperProto()
                wrapper.ParseFromString(msg[PROTO_PREFIX_LEN:])
                self._pending_response = wrapper
                for callback in self.message_received_callbacks:
                    callback(wrapper)
                self._proto_response_event.set()
        elif hex_msg.startswith("B313"):
            ack_body.extend(msg[2:4])
            ack_body.extend(bytes.fromhex("00000000000000"))
            wrapper = proto.WrapperProto()
            wrapper.ParseFromString(msg[PROTO_PREFIX_LEN:])
            for callback in self.message_received_callbacks:
                callback(wrapper)
            await self._handle_protobuf_request(wrapper)

        await self._acknowledge_message(msg, bytes(ack_body))

    async def _acknowledge_message(self, msg: bytes, response_body: bytes) -> None:
        await self.write_message(bytes.fromhex("8813") + msg[:2] + response_body)

    async def _handle_protobuf_request(self, wrapper: proto.WrapperProto) -> None:
        if not wrapper.HasField("event") or not wrapper.event.HasField("notification"):
            return

        notification = wrapper.event.notification.AlertNotification
        if notification.HasField("state") and self.state_callback is not None:
            self.state_callback(notification.state)
        if notification.HasField("metrics") and self.metrics_callback is not None:
            self.metrics_callback(notification.metrics)
        if notification.HasField("error") and self.error_callback is not None:
            self.error_callback(notification.error)
        if notification.HasField("tilt_calibration") and self.tilt_callback is not None:
            self.tilt_callback(notification.tilt_calibration)
