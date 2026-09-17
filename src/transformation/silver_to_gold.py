from __future__ import annotations

from hashlib import sha256
from pathlib import Path

import numpy as np
import pandas as pd

from schemas.gold_schema import (
    FACT_PROCESS_DAILY_COLUMNS,
    PROCESS_DESCRIPTOR_COLUMNS,
    PROCESS_IDENTITY_COLUMNS,
    PROCESS_STABLE_CONTEXT_COLUMNS,
    REQUIRED_SILVER_COLUMNS,
    TORQUE_SPEC_COLUMNS,
)
from schemas.silver_schema import PLANT_TIMEZONE


def read_silver(silver_root: str | Path) -> pd.DataFrame:
    """Lee todos los archivos Parquet de Silver sin depender del descubrimiento automático del dataset."""
    silver_root = Path(silver_root)
    parquet_files = sorted(silver_root.glob("**/*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No se encontraron archivos Parquet de Silver en {silver_root}")
    silver = pd.concat(
        [pd.read_parquet(path, engine="pyarrow") for path in parquet_files],
        ignore_index=True,
    )
    validate_silver_for_gold(silver)
    return silver


def validate_silver_for_gold(silver: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_SILVER_COLUMNS) - set(silver.columns))
    if missing:
        raise ValueError(f"Faltan columnas Silver requeridas por Gold: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(silver["event_timestamp"]):
        raise TypeError("event_timestamp debe ser una columna datetime de pandas")
    if silver["event_timestamp"].dt.tz is None:
        raise ValueError("event_timestamp debe incluir zona horaria y almacenarse en UTC")

    torque_status = set(silver["torque_spec_status"].dropna().astype(str).unique())
    allowed = {"IN_SPEC", "OUT_OF_SPEC", "NOT_APPLICABLE"}
    unexpected = sorted(torque_status - allowed)
    if unexpected:
        raise ValueError(f"Valores inesperados de torque_spec_status: {unexpected}")


def _stable_process_key(values: tuple[object, ...]) -> str:
    canonical = "|".join("<NA>" if pd.isna(value) else str(value) for value in values)
    return sha256(canonical.encode("utf-8")).hexdigest()


def add_gold_keys(silver: pd.DataFrame) -> pd.DataFrame:
    """Añade una clave de proceso determinista y claves de calendario local de planta."""
    result = silver.copy()
    result["process_key"] = [
        _stable_process_key(values)
        for values in result[PROCESS_IDENTITY_COLUMNS].itertuples(index=False, name=None)
    ]
    local_timestamp = result["event_timestamp"].dt.tz_convert(PLANT_TIMEZONE)
    result["local_date"] = local_timestamp.dt.date
    result["date_key"] = local_timestamp.dt.strftime("%Y%m%d").astype("int64")
    return result


def _assert_stable_process_context(events: pd.DataFrame) -> None:
    grouped = events.groupby("process_key", dropna=False)
    violations: dict[str, int] = {}
    for column in PROCESS_STABLE_CONTEXT_COLUMNS:
        counts = grouped[column].nunique(dropna=False)
        count = int(counts.gt(1).sum())
        if count:
            violations[column] = count
    if violations:
        raise ValueError(
            "La granularidad de proceso sensible a la configuración no es funcionalmente estable. "
            f"Incumplimientos por columna: {violations}. Revisa la identidad de proceso."
        )


def _primary_value(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    counts = non_null.astype("string").value_counts()
    max_count = counts.max()
    selected = sorted(counts[counts.eq(max_count)].index.tolist())[0]
    return non_null[non_null.astype("string").eq(selected)].iloc[0]


def _descriptor_variant_count(series: pd.Series) -> int:
    return int(series.nunique(dropna=True))


def _torque_spec_table(events: pd.DataFrame) -> pd.DataFrame:
    applicable = events.loc[events["torque_applicable"].fillna(False)].copy()
    if applicable.empty:
        return pd.DataFrame(columns=["process_key", *TORQUE_SPEC_COLUMNS])

    grouped = applicable.groupby("process_key", dropna=False)
    violations = {}
    for column in TORQUE_SPEC_COLUMNS:
        counts = grouped[column].nunique(dropna=False)
        count = int(counts.gt(1).sum())
        if count:
            violations[column] = count
    if violations:
        raise ValueError(
            "Las especificaciones de torque no son homogéneas dentro de la granularidad de proceso Gold. "
            f"Incumplimientos: {violations}. Introduce una configuración de proceso versionada "
            "si las especificaciones cambian legítimamente dentro de la misma identidad."
        )
    return grouped[TORQUE_SPEC_COLUMNS].first().reset_index()


def build_dim_process(silver: pd.DataFrame) -> pd.DataFrame:
    """Construye una fila por proceso analítico TU + PS + STEP sensible a la configuración."""
    events = add_gold_keys(silver)
    _assert_stable_process_context(events)

    base = (
        events.groupby("process_key", dropna=False)[
            [*PROCESS_IDENTITY_COLUMNS, *PROCESS_STABLE_CONTEXT_COLUMNS]
        ]
        .first()
        .reset_index()
    )

    descriptor_rows = []
    for process_key, group in events.groupby("process_key", dropna=False, sort=False):
        row = {"process_key": process_key}
        for column in PROCESS_DESCRIPTOR_COLUMNS:
            row[f"{column}_primary"] = _primary_value(group[column])
            row[f"{column}_variant_count"] = _descriptor_variant_count(group[column])
        descriptor_rows.append(row)

    descriptors = pd.DataFrame(descriptor_rows)
    specs = _torque_spec_table(events)
    result = base.merge(descriptors, on="process_key", how="left")
    result = result.merge(specs, on="process_key", how="left")
    result["has_torque_spec"] = result["target_torque"].notna()

    return result.sort_values(
        ["factory_id", "line_id", "tightening_unit_id", "process_step_id", "substep_id"],
        na_position="last",
    ).reset_index(drop=True)


def build_dim_date(silver: pd.DataFrame) -> pd.DataFrame:
    events = add_gold_keys(silver)
    dates = pd.to_datetime(pd.Series(sorted(events["local_date"].unique())))
    iso = dates.dt.isocalendar()
    return pd.DataFrame(
        {
            "date_key": dates.dt.strftime("%Y%m%d").astype("int64"),
            "local_date": dates.dt.date,
            "year": dates.dt.year.astype("int64"),
            "quarter": dates.dt.quarter.astype("int64"),
            "month": dates.dt.month.astype("int64"),
            "month_name": dates.dt.month_name(),
            "iso_week": iso.week.astype("int64"),
            "day_of_month": dates.dt.day.astype("int64"),
            "day_of_week": (dates.dt.dayofweek + 1).astype("int64"),
            "day_name": dates.dt.day_name(),
            "is_weekend": dates.dt.dayofweek.ge(5),
        }
    )


def _safe_rate(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = numerator.astype(float)
    denominator = denominator.astype(float)
    return np.where(denominator.gt(0), numerator / denominator * 100.0, np.nan)


def build_fact_process_daily(silver: pd.DataFrame) -> pd.DataFrame:
    """Construye KPIs diarios de torque a granularidad fecha + configuración de proceso.

    El OK/NOK de origen (`source_result_status`) no se agrega intencionalmente aquí.
    La conformidad del torque se deriva de forma independiente a partir del torque aplicado y de los
    límites mínimo/máximo configurados.
    """
    events = add_gold_keys(silver)

    torque_status = events["torque_spec_status"].astype("string")
    applicable = events["torque_applicable"].fillna(False).astype(bool)

    events["_torque_applicable"] = applicable
    events["_torque_not_applicable"] = ~applicable
    events["_torque_in_spec"] = torque_status.eq("IN_SPEC")
    events["_torque_out_of_spec"] = torque_status.eq("OUT_OF_SPEC")

    events["_applicable_torque"] = events["applied_torque"].where(applicable)
    events["_torque_delta"] = (
        events["applied_torque"] - events["target_torque"]
    ).where(applicable)
    events["_abs_torque_delta"] = events["_torque_delta"].abs()

    fact = (
        events.groupby(["date_key", "process_key"], dropna=False)
        .agg(
            event_count=("source_row_number", "size"),
            torque_applicable_count=("_torque_applicable", "sum"),
            torque_not_applicable_count=("_torque_not_applicable", "sum"),
            torque_in_spec_count=("_torque_in_spec", "sum"),
            torque_out_of_spec_count=("_torque_out_of_spec", "sum"),
            applied_torque_mean=("_applicable_torque", "mean"),
            applied_torque_median=("_applicable_torque", "median"),
            applied_torque_std=("_applicable_torque", "std"),
            applied_torque_min=("_applicable_torque", "min"),
            applied_torque_max=("_applicable_torque", "max"),
            mean_torque_delta_from_target=("_torque_delta", "mean"),
            mean_abs_torque_delta_from_target=("_abs_torque_delta", "mean"),
            first_event_timestamp_utc=("event_timestamp", "min"),
            last_event_timestamp_utc=("event_timestamp", "max"),
            source_file_count=("source_file", "nunique"),
        )
        .reset_index()
    )

    fact["torque_applicable_rate_pct"] = _safe_rate(
        fact["torque_applicable_count"], fact["event_count"]
    )
    fact["torque_in_spec_rate_pct"] = _safe_rate(
        fact["torque_in_spec_count"], fact["torque_applicable_count"]
    )
    fact["torque_out_of_spec_rate_pct"] = _safe_rate(
        fact["torque_out_of_spec_count"], fact["torque_applicable_count"]
    )

    fact = fact[FACT_PROCESS_DAILY_COLUMNS]
    return fact.sort_values(["date_key", "process_key"]).reset_index(drop=True)


def validate_gold_outputs(
    silver: pd.DataFrame,
    dim_process: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_process_daily: pd.DataFrame,
) -> None:
    if dim_process["process_key"].duplicated().any():
        raise ValueError("dim_process contiene valores process_key duplicados")
    if dim_date["date_key"].duplicated().any():
        raise ValueError("dim_date contiene valores date_key duplicados")
    if fact_process_daily[["date_key", "process_key"]].duplicated().any():
        raise ValueError("La granularidad de fact_process_daily no es única")
    if set(fact_process_daily["process_key"]) - set(dim_process["process_key"]):
        raise ValueError("fact_process_daily contiene process keys desconocidas")
    if set(fact_process_daily["date_key"]) - set(dim_date["date_key"]):
        raise ValueError("fact_process_daily contiene date keys desconocidas")
    if int(fact_process_daily["event_count"].sum()) != len(silver):
        raise ValueError("Gold event_count no reconcilia con el número de filas Silver")

    if not (
        fact_process_daily["torque_applicable_count"]
        + fact_process_daily["torque_not_applicable_count"]
    ).eq(fact_process_daily["event_count"]).all():
        raise ValueError("Los conteos de aplicabilidad de torque no reconcilian con event_count")

    if not (
        fact_process_daily["torque_in_spec_count"]
        + fact_process_daily["torque_out_of_spec_count"]
    ).eq(fact_process_daily["torque_applicable_count"]).all():
        raise ValueError("Los conteos de conformidad de torque no reconcilian con los eventos aplicables")


def build_gold_frames(silver: pd.DataFrame) -> dict[str, pd.DataFrame]:
    validate_silver_for_gold(silver)
    dim_process = build_dim_process(silver)
    dim_date = build_dim_date(silver)
    fact_process_daily = build_fact_process_daily(silver)
    validate_gold_outputs(silver, dim_process, dim_date, fact_process_daily)
    return {
        "dim_process": dim_process,
        "dim_date": dim_date,
        "fact_process_daily": fact_process_daily,
    }
