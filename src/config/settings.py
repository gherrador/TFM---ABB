from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


def _load_project_env() -> None:
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str

    @property
    def connection_kwargs(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


def get_postgres_settings() -> PostgresSettings:
    _load_project_env()

    settings = PostgresSettings(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "tfm_tightening"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        schema=os.getenv("POSTGRES_SCHEMA", "analytics"),
    )

    if not settings.password:
        raise ValueError(
            "POSTGRES_PASSWORD vacío. Configúralo en el archivo .env del proyecto."
        )

    return settings


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    schema_registry_url: str
    raw_topic: str
    security_protocol: str
    existing_tu_oos_probability: float
    new_tu_event_probability: float
    new_tu_oos_probability: float
    event_interval_seconds: float


def get_kafka_settings() -> KafkaSettings:
    _load_project_env()

    settings = KafkaSettings(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        raw_topic=os.getenv("KAFKA_RAW_TOPIC", "torque-events-raw"),
        security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        existing_tu_oos_probability=float(
            os.getenv("EXISTING_TU_OOS_PROBABILITY", "0.025")
        ),
        new_tu_event_probability=float(os.getenv("NEW_TU_EVENT_PROBABILITY", "0.03")),
        new_tu_oos_probability=float(os.getenv("NEW_TU_OOS_PROBABILITY", "0.01")),
        event_interval_seconds=float(os.getenv("STREAM_EVENT_INTERVAL_SECONDS", "0.5")),
    )

    probabilities = (
        settings.existing_tu_oos_probability,
        settings.new_tu_event_probability,
        settings.new_tu_oos_probability,
    )
    if any(value < 0 or value > 1 for value in probabilities):
        raise ValueError(
            "Las probabilidades del simulador Kafka deben estar entre 0 y 1."
        )
    if settings.event_interval_seconds < 0:
        raise ValueError("STREAM_EVENT_INTERVAL_SECONDS debe ser >= 0.")

    return settings
