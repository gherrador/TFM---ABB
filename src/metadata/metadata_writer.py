from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path


def write_metadata(
    output_path: str | Path,
    *,
    layer: str,
    source_file: str,
    input_records: int,
    output_records: int,
    quarantined_records: int,
    input_columns: int,
    output_columns: int,
    schema_version: str,
    processing_mode: str = "batch",
    additional_metadata: dict | None = None,
) -> None:

    metadata = {
        "layer": layer,
        "generated_at": datetime.now(
            timezone.utc
        ).isoformat(),

        "source_file": source_file,

        "source_format": "csv",
        "output_format": "parquet",

        "input_records": input_records,
        "output_records": output_records,
        "quarantined_records": quarantined_records,

        "input_columns": input_columns,
        "output_columns": output_columns,

        "schema_version": schema_version,
        "processing_mode": processing_mode,
    }

    if additional_metadata:
        metadata.update(additional_metadata)

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            metadata,
            file,
            indent=4,
            ensure_ascii=False,
        )