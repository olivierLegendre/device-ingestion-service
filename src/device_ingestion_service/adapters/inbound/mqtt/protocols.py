import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from device_ingestion_service.domain.entities import ParsedMeasurement
from device_ingestion_service.domain.errors import InvalidPayloadError, UnprocessablePayloadError

_SKIP_FIELDS = {
    "battery",
    "devEui",
    "dev_eui",
    "device_id",
    "f_cnt",
    "f_port",
    "frame_id",
    "last_seen",
    "linkquality",
    "messageId",
    "message_id",
    "port",
    "received_at",
    "rssi",
    "snr",
    "timestamp",
    "ts",
    "voltage",
}

_UNITS_BY_KEY = {
    "temperature": "degC",
    "humidity": "%",
    "pressure": "Pa",
    "co2": "ppm",
    "illuminance": "lux",
}


@dataclass(frozen=True)
class ProtocolEnvelope:
    topic: str
    payload: dict[str, Any]


class ProtocolParser:
    def parse(self, envelope: ProtocolEnvelope) -> list[ParsedMeasurement]:
        raise NotImplementedError


def _parse_observed_at(value: Any) -> datetime:
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 10_000_000_000:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=UTC)
    return datetime.now(tz=UTC)


def _message_id(payload: dict[str, Any], fallback_seed: str) -> str:
    explicit = payload.get("message_id") or payload.get("messageId") or payload.get("frame_id")
    if explicit:
        return str(explicit)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{fallback_seed}|{canonical}".encode()).hexdigest()[:24]


def _extract_measurements(
    payload: dict[str, Any],
) -> list[tuple[str, float | int | bool | str, str | None]]:
    items: list[tuple[str, float | int | bool | str, str | None]] = []
    for key, value in payload.items():
        if key in _SKIP_FIELDS:
            continue
        if isinstance(value, (float, int, bool, str)):
            items.append((key, value, _UNITS_BY_KEY.get(key)))
    return items


class Zigbee2MqttParser(ProtocolParser):
    def parse(self, envelope: ProtocolEnvelope) -> list[ParsedMeasurement]:
        payload = envelope.payload
        topic_parts = [part for part in envelope.topic.split("/") if part]
        if len(topic_parts) < 2:
            raise InvalidPayloadError("zigbee2mqtt topic must include device identifier")
        device_id = topic_parts[1]
        observed_at = _parse_observed_at(payload.get("timestamp") or payload.get("ts"))
        message_id = _message_id(payload, fallback_seed=envelope.topic)

        values = _extract_measurements(payload)
        if not values:
            raise UnprocessablePayloadError("zigbee2mqtt payload has no ingestible fields")

        return [
            ParsedMeasurement(
                source="zigbee2mqtt",
                message_id=message_id,
                device_id=device_id,
                point_key=key,
                value=value,
                observed_at=observed_at,
                unit=unit,
            )
            for key, value, unit in values
        ]


class LoRaWanParser(ProtocolParser):
    def parse(self, envelope: ProtocolEnvelope) -> list[ParsedMeasurement]:
        payload = envelope.payload
        uplink = payload.get("uplink_message", {})
        decoded = uplink.get("decoded_payload") or payload.get("decoded_payload") or {}

        if not isinstance(decoded, dict):
            raise InvalidPayloadError("lorawan decoded payload must be an object")

        device_id = (
            payload.get("device_id")
            or payload.get("dev_eui")
            or payload.get("devEui")
            or payload.get("end_device_ids", {}).get("dev_eui")
        )
        if not device_id:
            topic_parts = [part for part in envelope.topic.split("/") if part]
            device_id = topic_parts[1] if len(topic_parts) > 1 else "unknown-device"

        observed_at = _parse_observed_at(payload.get("received_at") or uplink.get("received_at"))
        message_id = _message_id(payload, fallback_seed=envelope.topic)

        values = _extract_measurements(decoded)
        if not values:
            raise UnprocessablePayloadError("lorawan payload has no ingestible fields")

        return [
            ParsedMeasurement(
                source="lorawan",
                message_id=message_id,
                device_id=str(device_id),
                point_key=key,
                value=value,
                observed_at=observed_at,
                unit=unit,
            )
            for key, value, unit in values
        ]
