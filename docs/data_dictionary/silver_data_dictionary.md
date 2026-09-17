# Diccionario de datos Silver

Bronze preserva las 75 columnas de origen. Silver selecciona **24 campos fuente** y los renombra de forma canónica.

| Campo Bronze | Campo Silver | Tipo | Significado |
|---|---|---|---|
| `FCT_ID` | `factory_id` | Int64 | Identificador de fábrica/sitio. |
| `FCT_Label` | `factory_name` | string | Nombre de fábrica/sitio. |
| `LIN_ID` | `line_id` | Int64 | Identificador de línea. |
| `LIN_Label` | `line_name` | string | Nombre de línea. |
| `WKA_ID` | `work_area_id` | Int64 | Identificador de área de trabajo. |
| `WKA_Label` | `work_area_name` | string | Nombre del área de trabajo. |
| `TU_ID` | `tightening_unit_id` | Int64 | Identificador de unidad de apriete/estación. |
| `TU_Name` | `tightening_unit_name` | string | Nombre de unidad de apriete. |
| `PS_ID` | `process_step_id` | Int64 | Identificador del Process Step u operación. |
| `PS_Number` | `process_step_number` | Int64 | Número PS de origen. |
| `PS_Comment` | `process_step_name` | string | Nombre legible de la operación. |
| `STEP_ID` | `substep_id` | Int64 | Identificador del subpaso dentro del PS. |
| `STEP_Number` | `substep_number` | Int64 | Número secuencial de subpaso. |
| `STEP_Type` | `substep_type` | Int64 | Código de tipo de subpaso. |
| `STEP_TorqueTarget` | `target_torque` | Float64 | Torque objetivo configurado. |
| `STEP_TorqueMinTolerance` | `min_torque` | Float64 | Límite inferior de torque. |
| `STEP_TorqueMaxTolerance` | `max_torque` | Float64 | Límite superior de torque. |
| `Tool_ID` | `tool_id` | string | Identificador de herramienta. |
| `TOOL_Number` | `tool_number` | string | Número de herramienta. |
| `TOOL_SerialNumber` | `tool_serial_number` | string | Número de serie físico de la herramienta. |
| `RES_ID` | `result_id` | string | Identificador del resultado fuente. |
| `RES_Report` | `source_result_status` | string | Estado OK/NOK del sistema fuente; solo referencia. |
| `RES_DateTime` | `event_timestamp` | datetime UTC | Timestamp canónico del evento. |
| `RES_FinalTorque` | `applied_torque` | Float64 | Torque final medido. |

## Campos técnicos y derivados

| Campo | Significado |
|---|---|
| `source_row_number` | Número de fila estable dentro del archivo fuente. |
| `source_file` | Nombre del archivo fuente. |
| `ingestion_timestamp` | Timestamp técnico de ingestión. |
| `torque_applicable` | Indica si el evento puede evaluarse por torque. |
| `torque_spec_status` | `IN_SPEC`, `OUT_OF_SPEC` o `NOT_APPLICABLE`. |
| `dq_is_valid` | Indica si el registro supera las reglas DQ. |
| `dq_error_codes` | Códigos DQ detectados. |

`source_result_status` no determina los KPIs de conformidad; la clasificación se calcula a partir de `applied_torque`, `min_torque` y `max_torque`.
