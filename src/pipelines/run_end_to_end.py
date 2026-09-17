from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.settings import get_postgres_settings
from pipelines.run_batch_pipeline import run_batch_pipeline
from pipelines.run_gold_pipeline import MANIFEST_NAME, run_gold_pipeline
from serving.postgres_loader import ensure_serving_ready


def _failed_sources(batch_summary) -> list[dict[str, object]]:
    if batch_summary.empty or "status" not in batch_summary.columns:
        return []
    failed = batch_summary.loc[batch_summary["status"].eq("FAILED")]
    return failed.to_dict("records")


def run_end_to_end(
    *,
    bronze_root: str | Path = "data/bronze",
    silver_root: str | Path = "data/silver",
    quarantine_root: str | Path = "data/quarantine",
    gold_root: str | Path = "data/gold",
    schema_sql_path: str | Path = "docker/postgres/init/001_schema.sql",
) -> dict[str, object]:
    """Ejecuta el camino batch reproducible desde Bronze hasta PostgreSQL.

    El runner es intencionalmente idempotente:
    - Bronze -> Silver omite los archivos con el contrato actual y reconstruye automáticamente desde Bronze
      las salidas semánticas antiguas.
    - Gold procesa únicamente archivos Silver nuevos después del bootstrap y realiza automáticamente
      una reconstrucción completa única cuando detecta una migración de versión de schema.
    - PostgreSQL migra automáticamente un schema analytics incompatible y utiliza el estado ETL
      para cargar únicamente particiones fact Gold que todavía no fueron publicadas.
    """

    bronze_root = Path(bronze_root)
    silver_root = Path(silver_root)
    quarantine_root = Path(quarantine_root)
    gold_root = Path(gold_root)
    schema_sql_path = Path(schema_sql_path)

    print("=" * 72)
    print("PIPELINE END-TO-END DEL TFM")
    print("Bronze -> Silver -> Gold -> PostgreSQL")
    print("=" * 72)

    print("\n[1/3] Bronze -> Silver")
    batch_summary = run_batch_pipeline(
        bronze_root=bronze_root,
        silver_root=silver_root,
        quarantine_root=quarantine_root,
        force=False,
    )
    print(batch_summary.to_string(index=False))

    failed = _failed_sources(batch_summary)
    if failed:
        raise RuntimeError(
            "Bronze -> Silver falló para uno o más archivos fuente: "
            + json.dumps(failed, ensure_ascii=False)
        )

    print("\n[2/3] Silver -> Gold")
    manifest_path = gold_root / MANIFEST_NAME
    gold_mode = "incremental" if manifest_path.exists() else "full-refresh"
    print(f"Modo Gold seleccionado automáticamente: {gold_mode}")

    gold_result = run_gold_pipeline(
        silver_root=silver_root,
        gold_root=gold_root,
        mode=gold_mode,
    )
    print(
        json.dumps(
            {
                "status": gold_result.get("status", "SUCCESS"),
                "new_silver_files": gold_result.get("new_silver_files"),
                "gold_schema_version": gold_result.get("gold_schema_version"),
            },
            indent=2,
            ensure_ascii=False,
        )
    )

    print("\n[3/3] Gold -> PostgreSQL")
    settings = get_postgres_settings()
    print(
        "Destino PostgreSQL:",
        f"{settings.host}:{settings.port}/{settings.database}",
    )

    postgres_result = ensure_serving_ready(
        gold_root=gold_root,
        schema_sql_path=schema_sql_path,
        settings=settings,
    )

    print(json.dumps(postgres_result, indent=2, ensure_ascii=False))
    print("\n" + "=" * 72)
    print("PIPELINE LISTO")
    print(f"Vista de serving unificada: {settings.schema}.vw_process_daily_kpis")
    print("Dashboard Power BI: dashboard/kpi_dashboard.pbix")
    print("El dashboard combina histórico batch y nuevos eventos streaming tras actualizar.")
    print("=" * 72)

    return {
        "batch": batch_summary.to_dict("records"),
        "gold_mode": gold_mode,
        "gold": gold_result,
        "postgres": postgres_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ejecuta automáticamente Bronze -> Silver -> Gold -> PostgreSQL."
    )
    parser.add_argument("--bronze-root", default="data/bronze")
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--quarantine-root", default="data/quarantine")
    parser.add_argument("--gold-root", default="data/gold")
    parser.add_argument(
        "--schema-sql",
        default="docker/postgres/init/001_schema.sql",
    )
    args = parser.parse_args()

    run_end_to_end(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        quarantine_root=args.quarantine_root,
        gold_root=args.gold_root,
        schema_sql_path=args.schema_sql,
    )


if __name__ == "__main__":
    main()
