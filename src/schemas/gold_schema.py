from __future__ import annotations

GOLD_SCHEMA_VERSION = "3.0.0"

PROCESS_IDENTITY_COLUMNS = [
    "tightening_unit_id",
    "process_step_id",
    "process_step_name",
    "substep_id",
    "substep_type",
]

PROCESS_STABLE_CONTEXT_COLUMNS = [
    "factory_id",
    "factory_name",
    "line_id",
    "line_name",
    "work_area_id",
    "work_area_name",
    "tightening_unit_name",
    "process_step_number",
    "substep_number",
]

PROCESS_DESCRIPTOR_COLUMNS = [
    "tool_id",
    "tool_number",
    "tool_serial_number",
]

TORQUE_SPEC_COLUMNS = [
    "target_torque",
    "min_torque",
    "max_torque",
]

REQUIRED_SILVER_COLUMNS = [
    "event_timestamp",
    "source_file",
    "source_row_number",
    "result_id",
    "applied_torque",
    "torque_applicable",
    "torque_spec_status",
    *PROCESS_IDENTITY_COLUMNS,
    *PROCESS_STABLE_CONTEXT_COLUMNS,
    *PROCESS_DESCRIPTOR_COLUMNS,
    *TORQUE_SPEC_COLUMNS,
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
