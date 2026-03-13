from device_ingestion_service.adapters.inbound.mqtt.worker import MqttIngestionWorker
from device_ingestion_service.main import create_runtime


def main() -> None:
    settings, use_cases = create_runtime()
    worker = MqttIngestionWorker(use_cases=use_cases, settings=settings)
    worker.run_forever()


if __name__ == "__main__":
    main()
