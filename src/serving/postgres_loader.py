from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from collections.abc import Iterable

import numpy as np
import pandas as pd

from config.settings import PostgresSettings, get_postgres_settings

DIM_PROCESS_COLUMNS = [
    "process_key",
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
    "tool_id_primary",
    "tool_id_variant_count",
    "tool_number_primary",
    "tool_number_variant_count",
    "tool_serial_number_primary",
    "tool_serial_number_variant_count",
    "target_torque",
    "min_torque",
    "max_torque",
    "has_torque_spec",
]

DIM_DATE_COLUMNS = [
    "date_key",
    "local_date",
    "year",
    "quarter",
    "month",
    "month_name",
    "iso_week",
    "day_of_month",
    "day_of_week",
    "day_name",
    "is_weekend",
]

FACT_PROCESS_DAILY_COLUMNS = [
    "date_key",
    "process_key",
    "event_count",
    "torque_applicable_count",
    "torque_not_applicable_count",
    "torque_applicable_rate_pct",
    "torque_in_spec_count",
    "torque_out_of_spec_count",
    "torque_in_spec_rate_pct",
    "torque_out_of_spec_rate_pct",
    "applied_torque_mean",
    "applied_torque_median",
    "applied_torque_std",
    "applied_torque_min",
    "applied_torque_max",
    "mean_torque_delta_from_target",
    "mean_abs_torque_delta_from_target",
    "first_event_timestamp_utc",
    "last_event_timestamp_utc",
    "source_file_count",
]


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            'El serving PostgreSQL requiere psycopg. Instálalo con: poetry add "psycopg[binary]"'
        ) from exc
    return psycopg


def _normalise_scalar(value):
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _iter_rows(frame: pd.DataFrame, columns: list[str]) -> Iterable[tuple[object, ...]]:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"Faltan columnas para la carga PostgreSQL: {missing}")
    for values in frame[columns].itertuples(index=False, name=None):
        yield tuple(_normalise_scalar(value) for value in values)


def _fact_files(gold_root: Path) -> list[Path]:
    files = sorted((gold_root / "fact_process_daily").glob("**/*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No se encontraron archivos fact Parquet en {gold_root / 'fact_process_daily'}"
        )
    return files


def read_gold(gold_root: str | Path) -> dict[str, pd.DataFrame]:
    gold_root = Path(gold_root)
    dim_process_path = gold_root / "dim_process.parquet"
    dim_date_path = gold_root / "dim_date.parquet"
    if not dim_process_path.exists():
        raise FileNotFoundError(dim_process_path)
    if not dim_date_path.exists():
        raise FileNotFoundError(dim_date_path)
    fact_files = _fact_files(gold_root)
    return {
        "dim_process": pd.read_parquet(dim_process_path),
        "dim_date": pd.read_parquet(dim_date_path),
        "fact_process_daily": pd.concat(
            [pd.read_parquet(p) for p in fact_files], ignore_index=True
        ),
    }


def apply_schema(connection, schema_sql_path: str | Path) -> None:
    sql = Path(schema_sql_path).read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(sql)


def _schema_is_compatible(connection, schema: str) -> bool:
    required = {
        "dim_process": {
            "process_step_id",
            "process_step_name",
            "substep_id",
            "tightening_unit_id",
            "tool_id_primary",
        },
        "fact_process_daily": {
            "torque_in_spec_count",
            "torque_out_of_spec_count",
            "torque_out_of_spec_rate_pct",
        },
        "etl_load_state": {"dataset", "source_path", "sha256"},
    }
    with connection.cursor() as cursor:
        for table, columns in required.items():
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table),
            )
            present = {row[0] for row in cursor.fetchall()}
            if not columns.issubset(present):
                return False
    return True


def _ensure_schema(connection, schema: str, schema_sql_path: str | Path) -> bool:
    """Aplica el schema y reemplaza automáticamente un schema de serving antiguo e incompatible."""
    if _schema_is_compatible(connection, schema):
        apply_schema(connection, schema_sql_path)
        return False

    with connection.cursor() as cursor:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    apply_schema(connection, schema_sql_path)
    return True


def _copy_frame(
    connection, schema: str, table: str, frame: pd.DataFrame, columns: list[str]
) -> None:
    statement = f"COPY {schema}.{table} ({', '.join(columns)}) FROM STDIN"
    with connection.cursor() as cursor:
        with cursor.copy(statement) as copy:
            for row in _iter_rows(frame, columns):
                copy.write_row(row)


def _upsert_frame(
    connection,
    schema: str,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
    conflict_columns: list[str],
) -> None:
    if frame.empty:
        return
    temp_table = f"_stage_{table}"
    quoted = ", ".join(columns)
    conflict = ", ".join(conflict_columns)
    update_columns = [c for c in columns if c not in conflict_columns]
    update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    with connection.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS pg_temp.{temp_table}")
        cursor.execute(
            f"CREATE TEMP TABLE {temp_table} (LIKE {schema}.{table} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
    _copy_frame(connection, "pg_temp", temp_table, frame, columns)
    with connection.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {schema}.{table} ({quoted}) SELECT {quoted} FROM {temp_table} "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {update_sql}"
        )
        cursor.execute(f"DROP TABLE IF EXISTS pg_temp.{temp_table}")


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative_fact_file(path: Path, gold_root: Path) -> str:
    return path.relative_to(gold_root).as_posix()


def _loaded_fact_state(connection, schema: str) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT source_path, sha256 FROM {schema}.etl_load_state WHERE dataset = 'fact_process_daily'"
        )
        return {row[0]: row[1] for row in cursor.fetchall()}


def _record_fact_state(
    connection,
    schema: str,
    source_path: str,
    checksum: str,
    rows: int,
    event_count: int,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {schema}.etl_load_state
                (dataset, source_path, sha256, row_count, event_count, loaded_at)
            VALUES ('fact_process_daily', %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (dataset, source_path)
            DO UPDATE SET sha256=EXCLUDED.sha256, row_count=EXCLUDED.row_count,
                          event_count=EXCLUDED.event_count, loaded_at=EXCLUDED.loaded_at
            """,
            (source_path, checksum, rows, event_count),
        )


def full_refresh_gold_to_postgres(
    gold_root: str | Path,
    schema_sql_path: str | Path,
    settings: PostgresSettings | None = None,
) -> dict[str, int]:
    settings = settings or get_postgres_settings()
    psycopg = _import_psycopg()
    frames = read_gold(gold_root)
    with psycopg.connect(**settings.connection_kwargs) as connection:
        _ensure_schema(connection, settings.schema, schema_sql_path)
        with connection.cursor() as cursor:
            cursor.execute(
                f"TRUNCATE TABLE {settings.schema}.fact_process_daily, {settings.schema}.dim_process, "
                f"{settings.schema}.dim_date, {settings.schema}.etl_load_state RESTART IDENTITY CASCADE"
            )
        _copy_frame(
            connection,
            settings.schema,
            "dim_process",
            frames["dim_process"],
            DIM_PROCESS_COLUMNS,
        )
        _copy_frame(
            connection,
            settings.schema,
            "dim_date",
            frames["dim_date"],
            DIM_DATE_COLUMNS,
        )
        gold_root_path = Path(gold_root)
        for fact_file in _fact_files(gold_root_path):
            fact = pd.read_parquet(fact_file)
            _copy_frame(
                connection,
                settings.schema,
                "fact_process_daily",
                fact,
                FACT_PROCESS_DAILY_COLUMNS,
            )
            _record_fact_state(
                connection,
                settings.schema,
                _relative_fact_file(fact_file, gold_root_path),
                _file_sha256(fact_file),
                len(fact),
                int(fact["event_count"].sum()),
            )
        with connection.cursor() as cursor:
            for table in ("dim_process", "dim_date", "fact_process_daily"):
                cursor.execute(f"ANALYZE {settings.schema}.{table}")
    return fetch_serving_metrics(settings)


def incremental_gold_to_postgres(
    gold_root: str | Path,
    schema_sql_path: str | Path,
    settings: PostgresSettings | None = None,
) -> dict[str, object]:
    settings = settings or get_postgres_settings()
    psycopg = _import_psycopg()
    gold_root = Path(gold_root)
    dim_process = pd.read_parquet(gold_root / "dim_process.parquet")
    dim_date = pd.read_parquet(gold_root / "dim_date.parquet")
    fact_files = _fact_files(gold_root)
    loaded_files: list[str] = []
    skipped_files: list[str] = []

    with psycopg.connect(**settings.connection_kwargs) as connection:
        migrated = _ensure_schema(connection, settings.schema, schema_sql_path)
        _upsert_frame(
            connection,
            settings.schema,
            "dim_process",
            dim_process,
            DIM_PROCESS_COLUMNS,
            ["process_key"],
        )
        _upsert_frame(
            connection,
            settings.schema,
            "dim_date",
            dim_date,
            DIM_DATE_COLUMNS,
            ["date_key"],
        )
        state = _loaded_fact_state(connection, settings.schema)

        for fact_file in fact_files:
            source_path = _relative_fact_file(fact_file, gold_root)
            checksum = _file_sha256(fact_file)
            if source_path in state:
                if state[source_path] != checksum:
                    raise RuntimeError(
                        "Una partición fact Gold ya cargada en PostgreSQL cambió: "
                        f"{source_path}. Ejecuta full-refresh para datos históricos revisados."
                    )
                skipped_files.append(source_path)
                continue
            fact = pd.read_parquet(fact_file)
            _upsert_frame(
                connection,
                settings.schema,
                "fact_process_daily",
                fact,
                FACT_PROCESS_DAILY_COLUMNS,
                ["date_key", "process_key"],
            )
            _record_fact_state(
                connection,
                settings.schema,
                source_path,
                checksum,
                len(fact),
                int(fact["event_count"].sum()),
            )
            loaded_files.append(source_path)

        with connection.cursor() as cursor:
            for table in ("dim_process", "dim_date", "fact_process_daily"):
                cursor.execute(f"ANALYZE {settings.schema}.{table}")

    return {
        "mode": "incremental",
        "schema_migrated": migrated,
        "loaded_fact_partitions": loaded_files,
        "skipped_fact_partitions": skipped_files,
        **fetch_serving_metrics(settings),
    }


def ensure_serving_ready(
    gold_root: str | Path,
    schema_sql_path: str | Path,
    settings: PostgresSettings | None = None,
) -> dict[str, object]:
    """Punto de entrada normal: migra automáticamente el schema antiguo y carga únicamente las particiones Gold faltantes."""
    result = incremental_gold_to_postgres(gold_root, schema_sql_path, settings)
    return {**result, "mode": "auto"}


def fetch_serving_metrics(settings: PostgresSettings | None = None) -> dict[str, int]:
    settings = settings or get_postgres_settings()
    psycopg = _import_psycopg()
    with psycopg.connect(**settings.connection_kwargs) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    (SELECT COUNT(*) FROM {settings.schema}.dim_process),
                    (SELECT COUNT(*) FROM {settings.schema}.dim_date),
                    (SELECT COUNT(*) FROM {settings.schema}.fact_process_daily),
                    (SELECT COALESCE(SUM(event_count),0) FROM {settings.schema}.fact_process_daily)
                """
            )
            row = cursor.fetchone()
    if row is None:
        raise RuntimeError("La consulta de métricas PostgreSQL no devolvió resultados")
    return {
        "dim_process": int(row[0]),
        "dim_date": int(row[1]),
        "fact_process_daily": int(row[2]),
        "gold_event_count": int(row[3]),
    }
