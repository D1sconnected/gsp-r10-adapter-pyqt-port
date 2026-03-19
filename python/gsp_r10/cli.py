from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gsp_r10.config import AppConfig
from gsp_r10.r10_client import R10Client


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Headless Python Garmin R10 connector")
    parser.add_argument(
        "--config",
        default="settings.json",
        help="Path to the existing settings.json/jsonc file",
    )
    parser.add_argument(
        "--scan-only",
        action="store_true",
        help="Validate config load and print startup info without connecting",
    )
    parser.add_argument(
        "--scan",
        action="store_true",
        help="Scan nearby BLE devices and print visible names and addresses",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose Python-side logging",
    )
    return parser


async def _run(args: argparse.Namespace) -> None:
    config_path = Path(args.config)
    config = AppConfig.from_file(config_path)
    if args.scan_only:
        logging.getLogger(__name__).info(
            "Loaded config for device '%s' from %s",
            config.bluetooth.bluetooth_device_name,
            config_path.resolve(),
        )
        return

    if args.scan:
        devices = await R10Client.scan(timeout=10.0)
        logger = logging.getLogger(__name__)
        if not devices:
            logger.info("No BLE devices were visible during the scan")
            return
        for device in devices:
            logger.info("BLE device: %s [%s]", device.name or "<no name>", device.address)
        return

    client = R10Client(config.bluetooth)
    await client.connect()
    try:
        await asyncio.Event().wait()
    finally:
        await client.disconnect()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)7s %(name)s || %(message)s",
    )
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
