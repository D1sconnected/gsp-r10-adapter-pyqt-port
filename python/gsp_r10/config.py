from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any


def _strip_json_comments(text: str) -> str:
    result: list[str] = []
    in_string = False
    escape = False
    i = 0
    while i < len(text):
        char = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_string:
            result.append(char)
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            i += 1
            continue

        if char == '"':
            in_string = True
            result.append(char)
            i += 1
            continue

        if char == "/" and nxt == "/":
            while i < len(text) and text[i] not in "\r\n":
                i += 1
            continue

        if char == "/" and nxt == "*":
            i += 2
            while i + 1 < len(text) and not (text[i] == "*" and text[i + 1] == "/"):
                i += 1
            i += 2
            continue

        result.append(char)
        i += 1

    return "".join(result)


@dataclass(slots=True)
class OpenConnectConfig:
    ip: str = "127.0.0.1"
    port: int = 921


@dataclass(slots=True)
class BluetoothConfig:
    enabled: bool = True
    bluetooth_device_name: str = "Approach R10"
    reconnect_interval: int = 10
    send_status_changes_to_gsp: bool = False
    auto_wake: bool = True
    calibrate_tilt_on_connect: bool = True
    debug_logging: bool = False
    altitude: float = 0.0
    humidity: float = 0.5
    temperature: float = 60.0
    air_density: float = 1.225
    tee_distance_in_feet: float = 7.0
    address: str | None = None


@dataclass(slots=True)
class AppConfig:
    open_connect: OpenConnectConfig = field(default_factory=OpenConnectConfig)
    bluetooth: BluetoothConfig = field(default_factory=BluetoothConfig)

    @classmethod
    def from_file(cls, path: str | Path) -> "AppConfig":
        resolved_path = resolve_config_path(path)
        raw = resolved_path.read_text(encoding="utf-8")
        data = json.loads(_strip_json_comments(raw))
        bluetooth = data.get("bluetooth", {})
        return cls(
            open_connect=OpenConnectConfig(
                ip=data.get("openConnect", {}).get("ip", "127.0.0.1"),
                port=int(data.get("openConnect", {}).get("port", 921)),
            ),
            bluetooth=BluetoothConfig(
                enabled=bool(bluetooth.get("enabled", True)),
                bluetooth_device_name=bluetooth.get("bluetoothDeviceName", "Approach R10"),
                reconnect_interval=int(bluetooth.get("reconnectInterval", 10)),
                send_status_changes_to_gsp=bool(bluetooth.get("sendStatusChangesToGSP", False)),
                auto_wake=bool(bluetooth.get("autoWake", True)),
                calibrate_tilt_on_connect=bool(bluetooth.get("calibrateTiltOnConnect", True)),
                debug_logging=bool(bluetooth.get("debugLogging", False)),
                altitude=float(bluetooth.get("altitude", 0.0)),
                humidity=float(bluetooth.get("humidity", 0.5)),
                temperature=float(bluetooth.get("temperature", 60.0)),
                air_density=float(bluetooth.get("airDensity", 1.225)),
                tee_distance_in_feet=float(bluetooth.get("teeDistanceInFeet", 7.0)),
                address=_coerce_optional_str(bluetooth.get("address") or bluetooth.get("bluetoothDeviceMac")),
            ),
        )


def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def resolve_config_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute() and candidate.exists():
        return candidate

    search_paths = [
        Path.cwd() / candidate,
        Path(__file__).resolve().parents[3] / candidate,
    ]

    for search_path in search_paths:
        if search_path.exists():
            return search_path

    raise FileNotFoundError(
        f"Could not find config file '{candidate}'. Tried: {', '.join(str(path) for path in search_paths)}"
    )
