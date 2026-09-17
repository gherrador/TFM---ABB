from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import time

import pandas as pd

from data_quality.quarantine import split_valid_and_quarantine
from data_quality.rules import apply_data_quality_rules
from metadata.metadata_writer import write_metadata
from processing.torque_business_rules import add_torque_classification
from schemas.silver_schema import (
    COLUMN_MAPPING,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    PLANT_TIMEZONE,
    SILVER_SCHEMA_VERSION,
    SILVER_SOURCE_COLUMNS,
    STRING_COLUMNS,
)


def read_bronze(path: str | Path) -> pd.DataFrame:
    """Lee el CSV Bronze inmutable sin alterar los valores de origen."""

    return pd.read_csv(
        path,
        sep=";",
        index_col=False,
    )


def add_source_row_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade procedencia determinista a nivel de fila dentro del archivo fuente inmutable.

    No se asume que `result_id` sea globalmente único; por ello, la combinación
    `source_file + source_row_number` puede utilizarse posteriormente como referencia estable
    del registro de origen en las capas Gold/serving.
    """

    result = df.copy()
    result["source_row_number"] = pd.Series(
        range(1, len(result) + 1),
        index=result.index,
        dtype="Int64",
    )
    return result


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    required = SILVER_SOURCE_COLUMNS + ["source_row_number"]
    missing = [column for column in required if column not in df.columns]

    if missing:
        raise ValueError(f"Faltan columnas Bronze obligatorias: {missing}")

    return df[required].copy()


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMN_MAPPING)


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    timestamp = (
        result["event_timestamp"]
        .astype(str)
        .str.replace(
            r" ([+-]\d{1,2})$",
            lambda match: f" {int(match.group(1)):+03d}:00",
            regex=True,
        )
    )

    result["event_timestamp"] = pd.to_datetime(
        timestamp,
        format="%Y-%m-%d %H:%M:%S %z",
        errors="coerce",
        utc=True,
    )

    for column in FLOAT_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(
                result[column],
                errors="coerce",
            )

    for column in INTEGER_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(
                result[column],
                errors="coerce",
            ).astype("Int64")

    for column in STRING_COLUMNS:
        if column in result.columns:
            result[column] = result[column].astype("string")

    return result


def add_metadata_columns(
    df: pd.DataFrame,
    source_file: str,
) -> pd.DataFrame:
    result = df.copy()
    result["source_file"] = source_file
    result["ingestion_timestamp"] = datetime.now(timezone.utc)
    return result


def _safe_output_stem(source_file: str) -> str:
    stem = Path(source_file).stem
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("_") or "source"


def _derive_single_local_partition(
    event_timestamp: pd.Series,
) -> tuple[int, int]:
    """
    Devuelve el único año/mes local de planta representado por un archivo fuente.

    El contrato batch actual espera un archivo fuente mensual. Fallar explícitamente
    es más seguro que escribir silenciosamente registros de meses diferentes en una única
    partición o sobrescribir otro archivo.
    """

    valid = event_timestamp.dropna()
    if valid.empty:
        raise ValueError("No se pudo determinar año/mes a partir de event_timestamp.")

    local = valid.dt.tz_convert(PLANT_TIMEZONE)
    periods = pd.DataFrame(
        {
            "year": local.dt.year,
            "month": local.dt.month,
        }
    ).drop_duplicates()

    if len(periods) != 1:
        values = periods.sort_values(["year", "month"]).to_dict("records")
        raise ValueError(
            "Un archivo fuente Bronze contiene más de un año/mes local de planta: "
            f"{values}. Divide la fuente antes de la ingestión batch."
        )

    row = periods.iloc[0]
    return int(row["year"]), int(row["month"])


def transform_bronze_to_silver(
    bronze_path: str | Path,
    silver_root: str | Path,
    quarantine_root: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_time = time.perf_counter()

    bronze_path = Path(bronze_path)
    silver_root = Path(silver_root)
    quarantine_root = Path(quarantine_root)

    bronze = read_bronze(bronze_path)
    input_records = len(bronze)
    input_columns = len(bronze.columns)

    transformed = add_source_row_number(bronze)
    transformed = select_columns(transformed)
    transformed = rename_columns(transformed)
    transformed = convert_types(transformed)
    transformed = add_metadata_columns(
        transformed,
        source_file=bronze_path.name,
    )

    transformed = add_torque_classification(transformed)

    validated = apply_data_quality_rules(transformed)
    silver, quarantine = split_valid_and_quarantine(validated)

    year, month = _derive_single_local_partition(transformed["event_timestamp"])

    silver_partition = silver_root / f"year={year}" / f"month={month:02d}"
    quarantine_partition = quarantine_root / f"year={year}" / f"month={month:02d}"

    silver_partition.mkdir(parents=True, exist_ok=True)
    quarantine_partition.mkdir(parents=True, exist_ok=True)

    output_stem = _safe_output_stem(bronze_path.name)
    silver_file = silver_partition / f"tightening_operations__{output_stem}.parquet"
    quarantine_file = quarantine_partition / f"rejected_records__{output_stem}.parquet"

    silver.to_parquet(
        silver_file,
        index=False,
        engine="pyarrow",
    )
    quarantine.to_parquet(
        quarantine_file,
        index=False,
        engine="pyarrow",
    )

    duration = time.perf_counter() - start_time

    valid_pct = round(len(silver) / input_records * 100, 3) if input_records else 0.0

    metadata_extra = {
        "partition": {
            "year": year,
            "month": month,
            "timezone_basis": PLANT_TIMEZONE,
        },
        "execution": {
            "duration_seconds": round(duration, 3),
        },
        "data_quality": {
            "valid_records": len(silver),
            "invalid_records": len(quarantine),
            "valid_records_pct": valid_pct,
        },
        "timestamp_contract": {
            "event_timestamp_storage": "UTC",
            "plant_timezone": PLANT_TIMEZONE,
        },
        "record_identity": {
            "source_reference": ["source_file", "source_row_number"],
            "result_id_globally_unique": False,
        },
        "source_projection": {
            "bronze_input_columns": input_columns,
            "silver_source_columns_selected": len(SILVER_SOURCE_COLUMNS),
            "selection_strategy": "contrato analítico explícito",
        },
    }

    metadata_name = f"_metadata__{output_stem}.json"

    write_metadata(
        silver_partition / metadata_name,
        layer="silver",
        source_file=bronze_path.name,
        input_records=input_records,
        output_records=len(silver),
        quarantined_records=len(quarantine),
        input_columns=input_columns,
        output_columns=len(silver.columns),
        schema_version=SILVER_SCHEMA_VERSION,
        additional_metadata=metadata_extra,
    )

    write_metadata(
        quarantine_partition / metadata_name,
        layer="quarantine",
        source_file=bronze_path.name,
        input_records=input_records,
        output_records=len(quarantine),
        quarantined_records=len(quarantine),
        input_columns=input_columns,
        output_columns=len(quarantine.columns),
        schema_version=SILVER_SCHEMA_VERSION,
        additional_metadata=metadata_extra,
    )

    return silver, quarantine
