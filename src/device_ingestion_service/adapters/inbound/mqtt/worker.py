import json
import logging
from datetime import UTC, datetime

import paho.mqtt.client as mqtt

from device_ingestion_service.application.use_cases import DeviceIngestionUseCases, IngestionRequest
from device_ingestion_service.settings import Settings

logger = logging.getLogger(__name__)


class MqttIngestionWorker:
    def __init__(self, *, use_cases: DeviceIngestionUseCases, settings: Settings) -> None:
        self._use_cases = use_cases
        self._settings = settings
        self._topics = [topic.strip() for topic in settings.mqtt_topics.split(",") if topic.strip()]

    def run_forever(self) -> None:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=self._settings.mqtt_client_id
        )
        client.on_connect = self._on_connect
        client.on_message = self._on_message

        logger.info(
            "mqtt_worker_starting",
            extra={
                "host": self._settings.mqtt_host,
                "port": self._settings.mqtt_port,
                "topics": self._topics,
            },
        )

        client.connect(self._settings.mqtt_host, self._settings.mqtt_port, keepalive=60)
        client.loop_forever()

    def _on_connect(self, client: mqtt.Client, userdata, flags, reason_code, properties) -> None:
        if int(reason_code) != 0:
            logger.error("mqtt_connect_failed", extra={"reason_code": int(reason_code)})
            return

        for topic in self._topics:
            client.subscribe(topic, qos=self._settings.mqtt_qos)
        logger.info("mqtt_connected", extra={"topics": self._topics})

    def _on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
        protocol = _protocol_from_topic(msg.topic)
        payload = _decode_payload(msg.payload)

        result = self._use_cases.ingest(
            IngestionRequest(
                organization_id=self._settings.mqtt_default_organization_id,
                site_id=self._settings.mqtt_default_site_id,
                protocol=protocol,
                topic=msg.topic,
                payload=payload,
                received_at=datetime.now(tz=UTC),
            )
        )

        logger.info(
            "mqtt_message_processed",
            extra={
                "protocol": protocol,
                "topic": msg.topic,
                "accepted": result.accepted_count,
                "duplicate": result.duplicate_count,
                "dead_letter": result.dead_letter_count,
            },
        )


def _decode_payload(raw_payload: bytes) -> dict[str, object]:
    try:
        decoded = json.loads(raw_payload.decode("utf-8"))
        if isinstance(decoded, dict):
            return decoded
        return {"value": decoded}
    except json.JSONDecodeError:
        return {"_raw_payload": raw_payload.decode("utf-8", errors="replace")}


def _protocol_from_topic(topic: str) -> str:
    parts = [part for part in topic.split("/") if part]
    if not parts:
        return "unknown"
    return parts[0].lower()
