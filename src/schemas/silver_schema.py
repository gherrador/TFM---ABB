"""Contrato canónico Silver para eventos de operaciones de apriete.

Bronze permanece inmutable y conserva todas las columnas fuente. Silver selecciona deliberadamente
solo los campos de negocio necesarios, además de trazabilidad técnica y campos derivados de conformidad.

Jerarquía de origen (de mayor a menor):
FCT -> LIN/WKA -> TU -> PS -> STEP -> evento de resultado.

Proyección final de origen: 24 de las 75 columnas Bronze.
"""

SILVER_SOURCE_COLUMNS = [
    "FCT_ID",
    "FCT_Label",
    "LIN_ID",
    "LIN_Label",
    "WKA_ID",
    "WKA_Label",
    "TU_ID",
    "TU_Name",
    "PS_ID",
    "PS_Number",
    "PS_Comment",
    "STEP_ID",
    "STEP_Number",
    "STEP_Type",
    "STEP_TorqueTarget",
    "STEP_TorqueMinTolerance",
    "STEP_TorqueMaxTolerance",
    "Tool_ID",
    "TOOL_Number",
    "TOOL_SerialNumber",
    "RES_ID",
    "RES_Report",
    "RES_DateTime",
    "RES_FinalTorque",
]

COLUMN_MAPPING = {
    "FCT_ID": "factory_id",
    "FCT_Label": "factory_name",
    "LIN_ID": "line_id",
    "LIN_Label": "line_name",
    "WKA_ID": "work_area_id",
    "WKA_Label": "work_area_name",
    "TU_ID": "tightening_unit_id",
    "TU_Name": "tightening_unit_name",
    "PS_ID": "process_step_id",
    "PS_Number": "process_step_number",
    "PS_Comment": "process_step_name",
    "STEP_ID": "substep_id",
    "STEP_Number": "substep_number",
    "STEP_Type": "substep_type",
    "STEP_TorqueTarget": "target_torque",
    "STEP_TorqueMinTolerance": "min_torque",
    "STEP_TorqueMaxTolerance": "max_torque",
    "Tool_ID": "tool_id",
    "TOOL_Number": "tool_number",
    "TOOL_SerialNumber": "tool_serial_number",
    "RES_ID": "result_id",
    "RES_Report": "source_result_status",
    "RES_DateTime": "event_timestamp",
    "RES_FinalTorque": "applied_torque",
}

FLOAT_COLUMNS = [
    "target_torque",
    "min_torque",
    "max_torque",
    "applied_torque",
]

INTEGER_COLUMNS = [
    "factory_id",
    "line_id",
    "work_area_id",
    "tightening_unit_id",
    "process_step_id",
    "process_step_number",
    "substep_id",
    "substep_number",
    "substep_type",
    "tool_id",
    "tool_number",
    "result_id",
    "source_row_number",
]

STRING_COLUMNS = [
    "factory_name",
    "line_name",
    "work_area_name",
    "tightening_unit_name",
    "process_step_name",
    "tool_serial_number",
    "source_result_status",
]

PLANT_TIMEZONE = "Europe/Madrid"
SILVER_SCHEMA_VERSION = "4.0.0"
