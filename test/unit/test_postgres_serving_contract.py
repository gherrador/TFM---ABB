from datetime import date

import numpy as np
import pandas as pd

from config.settings import get_postgres_settings
from serving.postgres_loader import (
    DIM_DATE_COLUMNS,
    DIM_PROCESS_COLUMNS,
    FACT_PROCESS_DAILY_COLUMNS,
    _iter_rows,
)


def test_postgres_settings_are_loaded_from_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "tfm_tightening")
    monkeypatch.setenv("POSTGRES_USER", "tfm_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "tfm_password")
    monkeypatch.setenv("POSTGRES_SCHEMA", "analytics")

    settings = get_postgres_settings()

    assert settings.host == "postgres"
    assert settings.port == 5432
    assert settings.database == "tfm_tightening"
    assert settings.user == "tfm_user"
    assert settings.password == "tfm_password"
    assert settings.schema == "analytics"


def test_copy_row_normalisation_handles_pandas_nulls_and_numpy_scalars():
    frame = pd.DataFrame(
        {
            "date_key": [np.int64(20250828)],
            "local_date": [date(2025, 8, 28)],
            "value": [np.nan],
        }
    )

    rows = list(_iter_rows(frame, ["date_key", "local_date", "value"]))

    assert rows == [(20250828, date(2025, 8, 28), None)]


def test_postgres_contract_column_lists_are_unique():
    assert len(DIM_PROCESS_COLUMNS) == len(set(DIM_PROCESS_COLUMNS))
    assert len(DIM_DATE_COLUMNS) == len(set(DIM_DATE_COLUMNS))
    assert len(FACT_PROCESS_DAILY_COLUMNS) == len(set(FACT_PROCESS_DAILY_COLUMNS))
