import pandas as pd

from processing.torque_business_rules import (
    add_torque_classification,
    torque_applicability_mask,
)
from transformation.bronze_to_silver import (
    add_source_row_number,
    convert_types,
)


def test_source_row_number_is_one_based_and_stable():
    df = pd.DataFrame({"value": [10, 20, 30]})
    result = add_source_row_number(df)

    assert result["source_row_number"].tolist() == [1, 2, 3]
    assert str(result["source_row_number"].dtype) == "Int64"


def test_event_timestamp_is_normalised_to_utc():
    df = pd.DataFrame(
        {
            "event_timestamp": [
                "2025-06-02 06:20:33 +2",
                "2025-11-02 06:20:33 +1",
            ]
        }
    )

    result = convert_types(df)

    assert isinstance(result["event_timestamp"].dtype, pd.DatetimeTZDtype)
    assert str(result["event_timestamp"].dt.tz) == "UTC"
    assert result.loc[0, "event_timestamp"].hour == 4
    assert result.loc[1, "event_timestamp"].hour == 5


def test_torque_applicability_is_not_same_as_data_quality():
    df = pd.DataFrame(
        {
            "target_torque": [10.0, 0.0, 10.0, 10.0],
            "min_torque": [9.0, 0.0, 9.0, 9.0],
            "max_torque": [11.0, 0.0, 11.0, 11.0],
            "applied_torque": [10.5, 2.0, 12.0, 0.0],
        }
    )

    mask = torque_applicability_mask(df)
    assert mask.tolist() == [True, False, True, False]

    classified = add_torque_classification(df)
    assert classified["torque_spec_status"].tolist() == [
        "IN_SPEC",
        "NOT_APPLICABLE",
        "OUT_OF_SPEC",
        "NOT_APPLICABLE",
    ]


def test_silver_source_projection_is_final_24_field_contract():
    from schemas.silver_schema import SILVER_SOURCE_COLUMNS, COLUMN_MAPPING

    assert len(SILVER_SOURCE_COLUMNS) == 24
    assert set(SILVER_SOURCE_COLUMNS) == set(COLUMN_MAPPING)
    assert {"RES_ID", "RES_Report", "RES_DateTime", "RES_FinalTorque"}.issubset(SILVER_SOURCE_COLUMNS)
    assert not {
        "RES_VIN", "RES_ResultNumber", "RES_StepNumber", "RES_ErrorCode",
        "RES_StopSource", "RES_CycleOKCount", "TU_Number", "TU_Comment"
    }.intersection(SILVER_SOURCE_COLUMNS)
