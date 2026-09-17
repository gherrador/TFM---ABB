from __future__ import annotations

import argparse
from pathlib import Path

from config.settings import get_postgres_settings
from serving.postgres_loader import (
    ensure_serving_ready,
    full_refresh_gold_to_postgres,
    incremental_gold_to_postgres,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carga las tablas analíticas Gold en la capa de serving PostgreSQL."
    )
    parser.add_argument("--gold-root", default="data/gold")
    parser.add_argument(
        "--schema-sql",
        default="docker/postgres/init/001_schema.sql",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "incremental", "full-refresh"],
        default="auto",
    )
    args = parser.parse_args()

    settings = get_postgres_settings()

    print(
        "Destino PostgreSQL:",
        f"{settings.host}:{settings.port}/{settings.database}",
    )
    print("Schema:", settings.schema)
    print("Modo:", args.mode)
    print()

    if args.mode == "full-refresh":
        result = full_refresh_gold_to_postgres(
            gold_root=Path(args.gold_root),
            schema_sql_path=Path(args.schema_sql),
            settings=settings,
        )
    elif args.mode == "incremental":
        result = incremental_gold_to_postgres(
            gold_root=Path(args.gold_root),
            schema_sql_path=Path(args.schema_sql),
            settings=settings,
        )
    else:
        result = ensure_serving_ready(
            gold_root=Path(args.gold_root),
            schema_sql_path=Path(args.schema_sql),
            settings=settings,
        )

    print("Carga Gold -> PostgreSQL completada.")
    for name, value in result.items():
        if isinstance(value, int):
            print(f"{name}: {value:,}")
        else:
            print(f"{name}: {value}")


if __name__ == "__main__":
    main()
