from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


CATALOG_COLUMNS = [
    "factory_id",
    "factory_name",
    "line_id",
    "line_name",
    "work_area_id",
    "work_area_name",
    "tightening_unit_id",
    "tightening_unit_name",
    "process_step_id",
    "process_step_number",
    "process_step_name",
    "substep_id",
    "substep_number",
    "substep_type",
    "target_torque",
    "min_torque",
    "max_torque",
    "tool_id",
    "tool_number",
    "tool_serial_number",
]


@dataclass(frozen=True)
class SimulatorProbabilities:
    existing_oos: float = 0.025
    new_tu_event: float = 0.03
    new_tu_oos: float = 0.01


NEW_TU_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "factory_id": "990001",
        "factory_name": "Fábrica simulada",
        "line_id": "990001",
        "line_name": "Línea demo de streaming",
        "work_area_id": "990001",
        "work_area_name": "Área demo de streaming",
        "tightening_unit_id": "990090",
        "tightening_unit_name": "TU90-DEMO-1",
        "process_step_id": "99009001",
        "process_step_number": "9001",
        "process_step_name": "DEMO-TIGHTENING-25NM",
        "substep_id": "990090011",
        "substep_number": "1",
        "substep_type": "21",
        "target_torque": 25.0,
        "min_torque": 23.0,
        "max_torque": 27.0,
        "tool_id": "990090",
        "tool_number": "90",
        "tool_serial_number": "SIM00090",
    },
    {
        "factory_id": "990001",
        "factory_name": "Fábrica simulada",
        "line_id": "990001",
        "line_name": "Línea demo de streaming",
        "work_area_id": "990001",
        "work_area_name": "Área demo de streaming",
        "tightening_unit_id": "990091",
        "tightening_unit_name": "TU91-DEMO-2",
        "process_step_id": "99009101",
        "process_step_number": "9101",
        "process_step_name": "DEMO-TIGHTENING-45NM",
        "substep_id": "990091011",
        "substep_number": "1",
        "substep_type": "21",
        "target_torque": 45.0,
        "min_torque": 42.0,
        "max_torque": 48.0,
        "tool_id": "990091",
        "tool_number": "91",
        "tool_serial_number": "SIM00091",
    },
)


def _to_optional_string(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _to_optional_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def load_process_catalog(silver_root: Path) -> list[dict[str, Any]]:
    """Construye un catálogo de procesos/configuraciones distintas a partir del histórico Silver en Parquet."""

    files = sorted(silver_root.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos Parquet de Silver en {silver_root}. "
            "Ejecuta primero el pipeline batch histórico."
        )

    frames: list[pd.DataFrame] = []
    for path in files:
        frame = pd.read_parquet(path)
        missing = [column for column in CATALOG_COLUMNS if column not in frame.columns]
        if missing:
            raise ValueError(f"{path} no es compatible con Silver. Faltan: {missing}")
        frames.append(frame[CATALOG_COLUMNS])

    catalog = pd.concat(frames, ignore_index=True)
    catalog = catalog[
        catalog["target_torque"].notna()
        & catalog["min_torque"].notna()
        & catalog["max_torque"].notna()
    ].copy()
    catalog = catalog[catalog["min_torque"] <= catalog["max_torque"]]

    # Conserva la frecuencia histórica de ocurrencia. Una configuración rara no debe
    # ser tan probable como otra que apareció miles de veces en producción.
    catalog = (
        catalog.groupby(CATALOG_COLUMNS, dropna=False)
        .size()
        .reset_index(name="_historical_weight")
    )

    if catalog.empty:
        raise ValueError("Silver no contiene configuraciones de proceso válidas.")

    records: list[dict[str, Any]] = []
    for record in catalog.to_dict(orient="records"):
        normalized: dict[str, Any] = {}
        for column in CATALOG_COLUMNS:
            value = record[column]
            if column in {"target_torque", "min_torque", "max_torque"}:
                normalized[column] = _to_optional_float(value)
            else:
                normalized[column] = _to_optional_string(value)
        normalized["_historical_weight"] = int(record["_historical_weight"])
        records.append(normalized)
    return records


def _generate_in_spec(config: dict[str, Any], rng: random.Random) -> float:
    minimum = float(config["min_torque"])
    maximum = float(config["max_torque"])
    target = float(config["target_torque"])
    sigma = max((maximum - minimum) / 6.0, abs(target) * 0.002, 1e-6)
    for _ in range(20):
        value = rng.gauss(target, sigma)
        if minimum <= value <= maximum:
            return value
    return rng.uniform(minimum, maximum)


def _generate_out_of_spec(config: dict[str, Any], rng: random.Random) -> float:
    minimum = float(config["min_torque"])
    maximum = float(config["max_torque"])
    span = max(maximum - minimum, abs(float(config["target_torque"])) * 0.02, 0.1)
    excursion = rng.uniform(0.03, 0.20) * span
    if rng.random() < 0.5:
        return minimum - excursion
    return maximum + excursion


def _is_torque_applicable_config(config: dict[str, Any]) -> bool:
    target = config.get("target_torque")
    minimum = config.get("min_torque")
    maximum = config.get("max_torque")
    return (
        target is not None
        and minimum is not None
        and maximum is not None
        and float(target) != 0.0
        and float(maximum) > float(minimum)
    )


def _choose_historical_config(
    historical_catalog: list[dict[str, Any]],
    rng: random.Random,
) -> dict[str, Any]:
    weights = [
        max(int(item.get("_historical_weight", 1)), 1) for item in historical_catalog
    ]
    selected = dict(rng.choices(historical_catalog, weights=weights, k=1)[0])
    selected.pop("_historical_weight", None)
    return selected


def historical_catalog_stats(
    historical_catalog: list[dict[str, Any]],
) -> dict[str, float | int]:
    total_weight = sum(
        max(int(item.get("_historical_weight", 1)), 1) for item in historical_catalog
    )
    not_applicable_weight = sum(
        max(int(item.get("_historical_weight", 1)), 1)
        for item in historical_catalog
        if not _is_torque_applicable_config(item)
    )
    return {
        "distinct_configurations": len(historical_catalog),
        "historical_event_weight": total_weight,
        "not_applicable_weight": not_applicable_weight,
        "not_applicable_rate_pct": (
            (not_applicable_weight / total_weight) * 100.0 if total_weight else 0.0
        ),
    }


def build_torque_event(
    historical_catalog: list[dict[str, Any]],
    probabilities: SimulatorProbabilities,
    rng: random.Random,
) -> dict[str, Any]:
    is_new_tu = rng.random() < probabilities.new_tu_event
    config = (
        dict(rng.choice(NEW_TU_CATALOG))
        if is_new_tu
        else _choose_historical_config(historical_catalog, rng)
    )

    torque_applicable = _is_torque_applicable_config(config)
    oos_probability = (
        probabilities.new_tu_oos if is_new_tu else probabilities.existing_oos
    )
    is_oos = torque_applicable and (rng.random() < oos_probability)

    if not torque_applicable:
        # Las operaciones que no son de apriete (por ejemplo, Aflojar) continúan siendo eventos válidos,
        # pero nunca se generan artificialmente como torque NOK/OOS.
        applied_torque = float(config["target_torque"] or 0.0)
    else:
        applied_torque = (
            _generate_out_of_spec(config, rng)
            if is_oos
            else _generate_in_spec(config, rng)
        )

    event = dict(config)
    event.update(
        {
            "result_id": str(uuid.uuid4()),
            "source_result_status": "NOK" if is_oos else "OK",
            "event_timestamp": datetime.now(timezone.utc),
            "applied_torque": round(applied_torque, 4),
        }
    )
    return event


def build_dq_test_events(
    historical_catalog: list[dict[str, Any]],
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Construye seis eventos intencionalmente inválidos para ejercitar las reglas DQ de Quarantine en Spark.

    Estos mensajes son válidos para Avro por diseño, pero semánticamente inválidos según
    la capa DQ de streaming. Esto permite que la prueba de concepto demuestre la diferencia entre
    validación de schema y validación de calidad de datos.
    """
    base = build_torque_event(
        historical_catalog,
        SimulatorProbabilities(existing_oos=0.0, new_tu_event=0.0, new_tu_oos=0.0),
        rng,
    )

    cases: list[tuple[str, dict[str, Any]]] = [
        ("MISSING_RESULT_ID", {"result_id": None}),
        ("MISSING_EVENT_TIMESTAMP", {"event_timestamp": None}),
        ("MISSING_TIGHTENING_UNIT", {"tightening_unit_id": None}),
        ("MISSING_PROCESS_STEP", {"process_step_id": None}),
        ("MISSING_SUBSTEP", {"substep_id": None}),
        (
            "INVALID_TORQUE_LIMITS",
            {
                "min_torque": float(base["max_torque"]) + 1.0,
                "max_torque": float(base["min_torque"]) - 1.0,
            },
        ),
    ]

    events: list[dict[str, Any]] = []
    for case_name, mutations in cases:
        event = dict(base)
        event["result_id"] = str(uuid.uuid4())
        event["event_timestamp"] = datetime.now(timezone.utc)
        event["process_step_name"] = f"DQ_TEST_{case_name}"
        event.update(mutations)
        events.append(event)
    return events
