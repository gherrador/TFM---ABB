from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil

import pandas as pd

from schemas.gold_schema import GOLD_SCHEMA_VERSION
from schemas.silver_schema import PLANT_TIMEZONE, SILVER_SCHEMA_VERSION
from transformation.silver_to_gold import build_gold_frames, read_silver

MANIFEST_NAME = "_gold_manifest.json"


def _silver_files(silver_root: Path) -> list[Path]:
    files = sorted(silver_root.glob("**/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos Parquet de Silver en {silver_root}"
        )
    return files


def _relative_file(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _partition_from_silver_path(path: Path) -> tuple[int, int]:
    year = month = None
    for part in path.parts:
        if part.startswith("year="):
            year = int(part.split("=", 1)[1])
        elif part.startswith("month="):
            month = int(part.split("=", 1)[1])
    if year is None or month is None:
        raise ValueError(
            f"El archivo Silver no está almacenado bajo carpetas de partición year=/month=: {path}"
        )
    return year, month


def _fact_partition_path(fact_root: Path, year: int, month: int) -> Path:
    return (
        fact_root / f"year={year}" / f"month={month:02d}" / "fact_process_daily.parquet"
    )


def _write_fact_month(fact: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fact.to_parquet(output, index=False, engine="pyarrow")


def _load_manifest(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _write_manifest(path: Path, manifest: dict) -> None:
    with path.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2, ensure_ascii=False)


def _merge_dim_date(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date_key"], keep="first")
    return combined.sort_values("date_key").reset_index(drop=True)


def _validate_existing_process(existing_row: pd.Series, new_row: pd.Series) -> None:
    stable_columns = [
        "tightening_unit_id",
        "process_step_id",
        "process_step_name",
        "substep_id",
        "substep_type",
        "factory_id",
        "factory_name",
        "line_id",
        "line_name",
        "work_area_id",
        "work_area_name",
        "tightening_unit_name",
        "process_step_number",
        "substep_number",
        "target_torque",
        "min_torque",
        "max_torque",
        "has_torque_spec",
    ]
    conflicts = []
    for column in stable_columns:
        old = existing_row[column]
        new_value = new_row[column]
        if pd.isna(old) and pd.isna(new_value):
            continue
        if old != new_value:
            conflicts.append(column)
    if conflicts:
        raise ValueError(
            "Un proceso materializado previamente cambió su contexto/especificación estable. "
            f"process_key={new_row['process_key']}, conflictos={conflicts}. "
            "Ejecuta un refresco completo o versiona la configuración del proceso."
        )


def _merge_dim_process(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return new.copy()
    existing_by_key = existing.set_index("process_key", drop=False)
    new_by_key = new.set_index("process_key", drop=False)
    for key in existing_by_key.index.intersection(new_by_key.index):
        _validate_existing_process(existing_by_key.loc[key], new_by_key.loc[key])

    new_only = new.loc[~new["process_key"].isin(existing["process_key"])]
    combined = pd.concat([existing, new_only], ignore_index=True)
    return combined.sort_values(
        [
            "factory_id",
            "line_id",
            "tightening_unit_id",
            "process_step_id",
            "substep_id",
        ],
        na_position="last",
    ).reset_index(drop=True)


def _build_manifest(
    *,
    silver_root: Path,
    gold_root: Path,
    processed_silver_files: list[str],
    previous_manifest: dict | None,
    added_files: list[str],
) -> dict:
    dim_process = pd.read_parquet(gold_root / "dim_process.parquet")
    dim_date = pd.read_parquet(gold_root / "dim_date.parquet")
    fact_files = sorted((gold_root / "fact_process_daily").glob("**/*.parquet"))
    fact_rows = 0
    event_count = 0
    for path in fact_files:
        frame = pd.read_parquet(path, columns=["event_count"])
        fact_rows += len(frame)
        event_count += int(frame["event_count"].sum())

    history = list((previous_manifest or {}).get("incremental_history", []))
    history.append(
        {
            "loaded_at": datetime.now(timezone.utc).isoformat(),
            "silver_files_added": added_files,
        }
    )

    return {
        "layer": "gold",
        "gold_schema_version": GOLD_SCHEMA_VERSION,
        "silver_schema_version": SILVER_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "plant_timezone": PLANT_TIMEZONE,
        "source": str(silver_root),
        "load_strategy": "particiones-mensuales-incrementales",
        "process_grain": (
            "tightening_unit_id + process_step_id + process_step_name + "
            "substep_id + substep_type"
        ),
        "processed_silver_files": sorted(processed_silver_files),
        "incremental_history": history[-50:],
        "tables": {
            "dim_process": {
                "path": str(gold_root / "dim_process.parquet"),
                "rows": len(dim_process),
                "grain": "una fila por proceso TU + PS + STEP sensible a la configuración",
            },
            "dim_date": {
                "path": str(gold_root / "dim_date.parquet"),
                "rows": len(dim_date),
                "grain": "una fila por fecha de calendario local de planta",
            },
            "fact_process_daily": {
                "path": str(gold_root / "fact_process_daily"),
                "files": [str(path) for path in fact_files],
                "rows": fact_rows,
                "reconciled_silver_events": event_count,
                "grain": "una fila por date_key + process_key",
            },
        },
    }


def _full_refresh(silver_root: Path, gold_root: Path) -> dict:
    if gold_root.exists():
        shutil.rmtree(gold_root)
    gold_root.mkdir(parents=True, exist_ok=True)

    silver_files = _silver_files(silver_root)
    silver = read_silver(silver_root)
    gold = build_gold_frames(silver)

    gold["dim_process"].to_parquet(
        gold_root / "dim_process.parquet", index=False, engine="pyarrow"
    )
    gold["dim_date"].to_parquet(
        gold_root / "dim_date.parquet", index=False, engine="pyarrow"
    )

    fact = gold["fact_process_daily"].copy()
    date_values = pd.to_datetime(fact["date_key"].astype(str), format="%Y%m%d")
    fact["_year"] = date_values.dt.year
    fact["_month"] = date_values.dt.month
    for (year, month), group in fact.groupby(["_year", "_month"], sort=True):
        output = _fact_partition_path(
            gold_root / "fact_process_daily", int(year), int(month)
        )
        _write_fact_month(group.drop(columns=["_year", "_month"]), output)

    processed = [_relative_file(path, silver_root) for path in silver_files]
    manifest = _build_manifest(
        silver_root=silver_root,
        gold_root=gold_root,
        processed_silver_files=processed,
        previous_manifest=None,
        added_files=processed,
    )
    _write_manifest(gold_root / MANIFEST_NAME, manifest)
    return manifest


def _manifest_matches_current_contract(manifest: dict | None) -> bool:
    return bool(
        manifest
        and manifest.get("gold_schema_version") == GOLD_SCHEMA_VERSION
        and manifest.get("silver_schema_version") == SILVER_SCHEMA_VERSION
    )


def _incremental(silver_root: Path, gold_root: Path) -> dict:
    manifest_path = gold_root / MANIFEST_NAME
    manifest = _load_manifest(manifest_path)

    # Camino de migración automática: los cambios semánticos/de schema reconstruyen Gold una vez desde Silver.
    if not _manifest_matches_current_contract(manifest):
        rebuilt = _full_refresh(silver_root, gold_root)
        return {**rebuilt, "status": "AUTO_FULL_REFRESH_SCHEMA_MIGRATION"}

    required = [gold_root / "dim_process.parquet", gold_root / "dim_date.parquet"]
    if any(not path.exists() for path in required):
        rebuilt = _full_refresh(silver_root, gold_root)
        return {**rebuilt, "status": "AUTO_FULL_REFRESH_MISSING_GOLD"}

    all_files = _silver_files(silver_root)
    processed = set(manifest.get("processed_silver_files", []))
    new_files = [
        path for path in all_files if _relative_file(path, silver_root) not in processed
    ]
    if not new_files:
        return {**manifest, "status": "NO_NEW_DATA", "new_silver_files": 0}

    partitions: dict[tuple[int, int], list[Path]] = {}
    for path in new_files:
        partitions.setdefault(_partition_from_silver_path(path), []).append(path)

    fact_root = gold_root / "fact_process_daily"
    conflicting_months = [
        (year, month)
        for year, month in partitions
        if _fact_partition_path(fact_root, year, month).exists()
    ]
    if conflicting_months:
        raise RuntimeError(
            "Los nuevos datos Silver apuntan a un mes Gold ya materializado: "
            f"{conflicting_months}. Utiliza full-refresh para datos históricos tardíos o revisados."
        )

    existing_dim_process = pd.read_parquet(gold_root / "dim_process.parquet")
    existing_dim_date = pd.read_parquet(gold_root / "dim_date.parquet")
    added_relative: list[str] = []

    for (year, month), files in sorted(partitions.items()):
        silver = pd.concat(
            [pd.read_parquet(path, engine="pyarrow") for path in files],
            ignore_index=True,
        )
        gold = build_gold_frames(silver)
        existing_dim_process = _merge_dim_process(
            existing_dim_process, gold["dim_process"]
        )
        existing_dim_date = _merge_dim_date(existing_dim_date, gold["dim_date"])
        output = _fact_partition_path(fact_root, year, month)
        _write_fact_month(gold["fact_process_daily"], output)
        added_relative.extend(_relative_file(path, silver_root) for path in files)

    existing_dim_process.to_parquet(
        gold_root / "dim_process.parquet", index=False, engine="pyarrow"
    )
    existing_dim_date.to_parquet(
        gold_root / "dim_date.parquet", index=False, engine="pyarrow"
    )

    processed.update(added_relative)
    updated_manifest = _build_manifest(
        silver_root=silver_root,
        gold_root=gold_root,
        processed_silver_files=sorted(processed),
        previous_manifest=manifest,
        added_files=added_relative,
    )
    updated_manifest["status"] = "SUCCESS"
    updated_manifest["new_silver_files"] = len(added_relative)
    _write_manifest(manifest_path, updated_manifest)
    return updated_manifest


def run_gold_pipeline(
    silver_root: str | Path,
    gold_root: str | Path,
    *,
    mode: str = "incremental",
    force: bool | None = None,
) -> dict:
    silver_root = Path(silver_root)
    gold_root = Path(gold_root)
    if force is True:
        mode = "full-refresh"
    if mode == "full-refresh":
        return _full_refresh(silver_root, gold_root)
    if mode == "incremental":
        return _incremental(silver_root, gold_root)
    raise ValueError("mode debe ser 'incremental' o 'full-refresh'")


def main() -> None:
    parser = argparse.ArgumentParser(description="Construye Gold a partir de Silver")
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--gold-root", default="data/gold")
    parser.add_argument(
        "--mode", choices=["incremental", "full-refresh"], default="incremental"
    )
    args = parser.parse_args()
    manifest = run_gold_pipeline(
        silver_root=args.silver_root, gold_root=args.gold_root, mode=args.mode
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
