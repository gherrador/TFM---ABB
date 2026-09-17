import pandas as pd

from transformation.silver_to_gold import build_gold_frames


def _sample_silver() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "event_timestamp": pd.to_datetime(
                ["2025-06-01T08:00:00Z", "2025-06-01T08:01:00Z", "2025-06-01T08:02:00Z", "2025-06-02T08:00:00Z"],
                utc=True,
            ),
            "source_file": ["a.csv"] * 4,
            "source_row_number": pd.Series([1, 2, 3, 4], dtype="Int64"),
            "result_id": pd.Series([1, 2, 3, 4], dtype="Int64"),
            "applied_torque": [10.0, 12.0, 2.0, 10.5],
            "torque_applicable": [True, True, False, True],
            "torque_spec_status": pd.Series(["IN_SPEC", "OUT_OF_SPEC", "NOT_APPLICABLE", "IN_SPEC"], dtype="string"),
            "factory_id": pd.Series([777697] * 4, dtype="Int64"),
            "factory_name": pd.Series(["SAN FERNANDO DE HENARES"] * 4, dtype="string"),
            "line_id": pd.Series([952890] * 4, dtype="Int64"),
            "line_name": pd.Series(["PREMONTAJES"] * 4, dtype="string"),
            "work_area_id": pd.Series([975603] * 4, dtype="Int64"),
            "work_area_name": pd.Series(["BANDEJAS Y PREMONTAJES MECANICOS"] * 4, dtype="string"),
            "tightening_unit_id": pd.Series([8112363] * 4, dtype="Int64"),
            "tightening_unit_name": pd.Series(["TU63-EIBS7-1"] * 4, dtype="string"),
            "process_step_id": pd.Series([811236312] * 4, dtype="Int64"),
            "process_step_number": pd.Series([12] * 4, dtype="Int64"),
            "process_step_name": pd.Series(["INV-3.X-BANDEJAS-3"] * 4, dtype="string"),
            "substep_id": pd.Series([112363121] * 4, dtype="Int64"),
            "substep_number": pd.Series([1] * 4, dtype="Int64"),
            "substep_type": pd.Series([21] * 4, dtype="Int64"),
            "tool_id": pd.Series([100] * 4, dtype="Int64"),
            "tool_number": pd.Series([63] * 4, dtype="Int64"),
            "tool_serial_number": pd.Series(["T1", "T1", "T2", "T2"], dtype="string"),
            "target_torque": [10.0, 10.0, 0.0, 10.0],
            "min_torque": [9.0, 9.0, 0.0, 9.0],
            "max_torque": [11.0, 11.0, 0.0, 11.0],
        }
    )


def test_gold_reconciles_silver_and_preserves_denominators():
    gold = build_gold_frames(_sample_silver())
    fact = gold["fact_process_daily"]
    assert fact["event_count"].sum() == 4
    assert fact["torque_applicable_count"].sum() == 3
    assert fact["torque_in_spec_count"].sum() == 2
    assert fact["torque_out_of_spec_count"].sum() == 1
    assert fact["torque_not_applicable_count"].sum() == 1


def test_gold_dimension_uses_tu_ps_substep_process_identity():
    gold = build_gold_frames(_sample_silver())
    process = gold["dim_process"]
    assert len(process) == 1
    assert process.loc[0, "process_step_name"] == "INV-3.X-BANDEJAS-3"
    assert process.loc[0, "tool_serial_number_variant_count"] == 2
    assert process.loc[0, "tool_id_primary"] == 100
    assert process.loc[0, "target_torque"] == 10.0
    assert bool(process.loc[0, "has_torque_spec"]) is True


def test_daily_rates_use_torque_denominators_only():
    gold = build_gold_frames(_sample_silver())
    first_day = gold["fact_process_daily"].sort_values("date_key").iloc[0]
    assert first_day["event_count"] == 3
    assert first_day["torque_applicable_count"] == 2
    assert first_day["torque_out_of_spec_count"] == 1
    assert first_day["torque_out_of_spec_rate_pct"] == 50.0


def test_process_key_distinguishes_reused_ps_ids_with_different_operation_semantics():
    silver = _sample_silver().iloc[:2].copy()
    alternate = silver.iloc[[0]].copy()
    alternate["process_step_name"] = pd.Series(["Aflojar"], index=alternate.index, dtype="string")
    alternate["substep_type"] = pd.Series([0], index=alternate.index, dtype="Int64")
    alternate["target_torque"] = 0.0
    alternate["min_torque"] = 0.0
    alternate["max_torque"] = 0.0
    alternate["applied_torque"] = 2.0
    alternate["torque_applicable"] = False
    alternate["torque_spec_status"] = pd.Series(["NOT_APPLICABLE"], index=alternate.index, dtype="string")
    alternate["source_row_number"] = pd.Series([99], index=alternate.index, dtype="Int64")
    combined = pd.concat([silver, alternate], ignore_index=True)

    gold = build_gold_frames(combined)
    assert len(gold["dim_process"]) == 2
    assert set(gold["dim_process"]["process_step_name"].astype(str)) == {
        "INV-3.X-BANDEJAS-3",
        "Aflojar",
    }
