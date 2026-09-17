from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from pathlib import Path
from typing import Any

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.settings import get_kafka_settings
from iot.torque_event_simulator import (
    SimulatorProbabilities,
    build_dq_test_events,
    build_torque_event,
    historical_catalog_stats,
    load_process_catalog,
)


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"[ERROR DE ENTREGA] {err}", file=sys.stderr)
        return
    print(
        f"[ENTREGADO] topic={msg.topic()} partition={msg.partition()} "
        f"offset={msg.offset()}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publica eventos simulados de apriete en Kafka utilizando Avro."
    )
    parser.add_argument("--silver-root", type=Path, default=Path("data/silver"))
    parser.add_argument("--schema", type=Path, default=Path("src/schemas/torque_event.avsc"))
    parser.add_argument("--count", type=int, default=0, help="0 = ejecutar de forma continua")
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--inject-dq",
        action="store_true",
        help="Publica exactamente seis eventos de prueba DQ intencionalmente inválidos.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_kafka_settings()
    rng = random.Random(args.seed)
    interval = (
        settings.event_interval_seconds if args.interval is None else args.interval
    )
    if args.count < 0 or interval < 0:
        raise ValueError("--count y --interval deben ser >= 0.")

    schema_str = args.schema.read_text(encoding="utf-8")
    catalog = load_process_catalog(args.silver_root)
    stats = historical_catalog_stats(catalog)
    print(
        "[CATÁLOGO] "
        f"cargadas {stats['distinct_configurations']} configuraciones históricas distintas "
        f"ponderadas por {stats['historical_event_weight']} eventos históricos"
    )
    print(
        "[CATÁLOGO] proporción histórica NOT_APPLICABLE="
        f"{stats['not_applicable_rate_pct']:.2f}%"
    )

    schema_registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry,
        schema_str=schema_str,
    )
    producer = SerializingProducer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "security.protocol": settings.security_protocol,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    probabilities = SimulatorProbabilities(
        existing_oos=settings.existing_tu_oos_probability,
        new_tu_event=settings.new_tu_event_probability,
        new_tu_oos=settings.new_tu_oos_probability,
    )

    stop = False

    def request_stop(*_: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)

    sent = 0
    dq_events = build_dq_test_events(catalog, rng) if args.inject_dq else None

    if dq_events is not None:
        print("[PRUEBA DQ] publicando 6 eventos intencionalmente inválidos")
        event_source = dq_events
    else:
        event_source = None

    while not stop:
        if event_source is not None:
            if sent >= len(event_source):
                break
            event = event_source[sent]
        else:
            if args.count != 0 and sent >= args.count:
                break
            event = build_torque_event(catalog, probabilities, rng)

        producer.produce(
            topic=settings.raw_topic,
            key=event.get("tightening_unit_id") or "DQ-TEST",
            value=event,
            on_delivery=_delivery_report,
        )
        producer.poll(0)
        sent += 1
        torque = event.get("applied_torque")
        torque_text = "null" if torque is None else f"{torque:.4f}"
        print(
            "[EVENTO] "
            f"#{sent} resultado={event.get('result_id')} "
            f"tu={event.get('tightening_unit_name')} "
            f"proceso={event.get('process_step_name')} "
            f"torque={torque_text} "
            f"estado={event.get('source_result_status')}"
        )
        if interval:
            time.sleep(interval)

    remaining = producer.flush(10)
    if remaining:
        raise RuntimeError(f"{remaining} mensaje(s) Kafka no fueron entregados.")
    print(f"[FINALIZADO] publicados {sent} evento(s) en {settings.raw_topic}")


if __name__ == "__main__":
    main()
