import pandas as pd
import pytest

from data_quality.rules import apply_data_quality_rules


def _valid_row():
    return {
        "event_timestamp": pd.Timestamp("2025-06-01T10:00:00Z"),
        "tool_serial_number": "TOOL-1",
        "applied_torque": 10.0,
        "min_torque": 9.0,
        "target_torque": 10.0,
        "max_torque": 11.0,
        "source_result_status": "OK",
    }


def test_valid_event_passes_dq():
    result = apply_data_quality_rules(pd.DataFrame([_valid_row()]))

    assert bool(result.loc[0, "dq_is_valid"])
    assert result.loc[0, "dq_error_codes"] == ""


@pytest.mark.parametrize(
    ("field", "value", "expected_error"),
    [
        ("event_timestamp", pd.NaT, "DQ001_INVALID_TIMESTAMP"),
        ("tool_serial_number", None, "DQ002_MISSING_TOOL"),
        ("applied_torque", -1.0, "DQ003_NEGATIVE_TORQUE"),
        ("source_result_status", "UNKNOWN", "DQ005_INVALID_SOURCE_STATUS"),
    ],
)
def test_individual_dq_rules_are_detected(field, value, expected_error):
    row = _valid_row()
    row[field] = value

    result = apply_data_quality_rules(pd.DataFrame([row]))

    assert not bool(result.loc[0, "dq_is_valid"])
    assert expected_error in result.loc[0, "dq_error_codes"]


def test_blank_tool_serial_is_invalid():
    row = _valid_row()
    row["tool_serial_number"] = "   "

    result = apply_data_quality_rules(pd.DataFrame([row]))

    assert not bool(result.loc[0, "dq_is_valid"])
    assert "DQ002_MISSING_TOOL" in result.loc[0, "dq_error_codes"]


def test_invalid_range_is_quarantinable():
    row = _valid_row()
    row["min_torque"] = 12.0

    result = apply_data_quality_rules(pd.DataFrame([row]))

    assert not bool(result.loc[0, "dq_is_valid"])
    assert "DQ004_INVALID_TORQUE_RANGE" in result.loc[0, "dq_error_codes"]


def test_multiple_dq_errors_are_accumulated():
    row = _valid_row()
    row["event_timestamp"] = pd.NaT
    row["applied_torque"] = -5.0

    result = apply_data_quality_rules(pd.DataFrame([row]))

    assert not bool(result.loc[0, "dq_is_valid"])
    assert (
        result.loc[0, "dq_error_codes"]
        == "DQ001_INVALID_TIMESTAMP|DQ003_NEGATIVE_TORQUE"
    )
