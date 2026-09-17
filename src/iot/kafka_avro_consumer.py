from __future__ import annotations

import argparse
import json
from typing import Any

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from config.settings import get_kafka_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lee y decodifica eventos Avro de apriete desde Kafka."
    )
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--group-id", default="tfm-avro-verifier")
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args()


def _json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def main() -> None:
    args = parse_args()
    settings = get_kafka_settings()

    schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry)

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "security.protocol": settings.security_protocol,
            "group.id": args.group_id,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
        }
    )

    consumer.subscribe([settings.raw_topic])
    read = 0
    try:
        while read < args.count:
            message = consumer.poll(10.0)
            if message is None:
                print("[ESPERA] no se recibió ningún mensaje en 10 segundos")
                continue
            if message.error():
                raise RuntimeError(message.error())

            read += 1
            print(
                f"[MENSAJE {read}] partición={message.partition()} "
                f"offset={message.offset()} clave={message.key()}"
            )
            print(json.dumps(message.value(), indent=2, default=_json_default))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
