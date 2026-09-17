from pathlib import Path
import re

import pandas as pd

from schemas.silver_schema import PLANT_TIMEZONE, COLUMN_MAPPING
from transformation.bronze_to_silver import transform_bronze_to_silver


def _parse_source_timestamp(values: pd.Series) -> pd.Series:
    normalised = values.astype(str).str.replace(
        r" ([+-]\d{1,2})$",
        lambda match: f" {int(match.group(1)):+03d}:00",
        regex=True,
    )
    return pd.to_datetime(
        normalised,
        format="%Y-%m-%d %H:%M:%S %z",
        errors="coerce",
        utc=True,
    )


def _safe_output_stem(source_file: str) -> str:
    stem = Path(source_file).stem
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("_") or "source"


def _source_partition_and_output(
    csv_file: Path,
    silver_root: Path,
) -> Path:
    bronze = pd.read_csv(
        csv_file,
        sep=";",
        index_col=False,
        usecols=["RES_DateTime"],
    )

    timestamp = _parse_source_timestamp(bronze["RES_DateTime"])
    valid = timestamp.dropna()

    if valid.empty:
        raise ValueError(
            "No se pudo determinar la partición de origen a partir de RES_DateTime."
        )

    local = valid.dt.tz_convert(PLANT_TIMEZONE)
    periods = pd.DataFrame(
        {"year": local.dt.year, "month": local.dt.month}
    ).drop_duplicates()

    if len(periods) != 1:
        raise ValueError(
            "Un archivo fuente Bronze contiene más de un año/mes local de planta: "
            f"{periods.sort_values(['year', 'month']).to_dict('records')}"
        )

    year = int(periods.iloc[0]["year"])
    month = int(periods.iloc[0]["month"])
    output_stem = _safe_output_stem(csv_file.name)

    return (
        silver_root
        / f"year={year}"
        / f"month={month:02d}"
        / f"tightening_operations__{output_stem}.parquet"
    )


def run_batch_pipeline(
    bronze_root: str | Path,
    silver_root: str | Path,
    quarantine_root: str | Path,
    force: bool = False,
) -> pd.DataFrame:
    bronze_root = Path(bronze_root)
    silver_root = Path(silver_root)

    csv_files = sorted(bronze_root.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en {bronze_root}")

    results = []

    for csv_file in csv_files:
        print(f"Procesando: {csv_file.name}")

        try:
            silver_file = _source_partition_and_output(
                csv_file=csv_file,
                silver_root=silver_root,
            )

            legacy_file = silver_file.parent / "tightening_operations.parquet"
            if legacy_file.exists():
                print(
                    f"  Eliminando salida Silver antigua antes de reconstruir schema: {legacy_file}"
                )
                legacy_file.unlink()

            if silver_file.exists() and not force:
                existing_columns = set(
                    pd.read_parquet(silver_file, engine="pyarrow").columns
                )
                required_columns = set(COLUMN_MAPPING.values()) | {
                    "source_row_number",
                    "source_file",
                    "ingestion_timestamp",
                    "torque_applicable",
                    "torque_spec_status",
                    "dq_is_valid",
                    "dq_error_codes",
                }
                if required_columns.issubset(existing_columns):
                    print("  Ya procesado con el contrato Silver actual. Se omite.")
                    results.append(
                        {
                            "source_file": csv_file.name,
                            "status": "SKIPPED",
                            "silver_records": None,
                            "quarantine_records": None,
                            "error": None,
                        }
                    )
                    continue
                print(
                    "  Se detectó una salida Silver antigua/incompatible. Se reconstruye desde la fuente."
                )

            silver, quarantine = transform_bronze_to_silver(
                bronze_path=csv_file,
                silver_root=silver_root,
                quarantine_root=quarantine_root,
            )

            results.append(
                {
                    "source_file": csv_file.name,
                    "status": "SUCCESS",
                    "silver_records": len(silver),
                    "quarantine_records": len(quarantine),
                    "error": None,
                }
            )

        except Exception as exc:
            results.append(
                {
                    "source_file": csv_file.name,
                    "status": "FAILED",
                    "silver_records": None,
                    "quarantine_records": None,
                    "error": str(exc),
                }
            )

    return pd.DataFrame(results)
