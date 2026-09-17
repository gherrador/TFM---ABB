# Proyección Bronze → Silver

Bronze contiene 75 columnas de origen. Silver promueve exactamente 24 y mantiene las 51 restantes únicamente en la capa Bronze inmutable.

## Campos retenidos (24)

- `FCT_ID`
- `FCT_Label`
- `LIN_ID`
- `LIN_Label`
- `WKA_ID`
- `WKA_Label`
- `TU_ID`
- `TU_Name`
- `PS_ID`
- `PS_Number`
- `PS_Comment`
- `STEP_ID`
- `STEP_Number`
- `STEP_Type`
- `STEP_TorqueTarget`
- `STEP_TorqueMinTolerance`
- `STEP_TorqueMaxTolerance`
- `Tool_ID`
- `TOOL_Number`
- `TOOL_SerialNumber`
- `RES_ID`
- `RES_Report`
- `RES_DateTime`
- `RES_FinalTorque`

## Criterio de selección

La proyección conserva la jerarquía de fábrica/proceso, la configuración de torque, los descriptores de herramienta necesarios para análisis futuros y los campos mínimos del resultado necesarios para identificar, fechar y evaluar el evento.

Los campos de controlador, versiones de configuración, variables angulares, tendencias, corrientes y diagnósticos auxiliares permanecen disponibles en Bronze, pero no se promueven al contrato analítico Silver porque no son necesarios para los KPIs definidos en el alcance final.

## Renombrado canónico

Los nombres de origen se transforman a nombres canónicos. Ejemplos:

```text
FCT_ID                  → factory_id
TU_ID                   → tightening_unit_id
PS_Comment              → process_step_name
STEP_TorqueTarget       → target_torque
RES_Report              → source_result_status
RES_DateTime            → event_timestamp
RES_FinalTorque         → applied_torque
```

## Campos derivados en Silver

Además de los 24 campos fuente, Silver añade trazabilidad y semántica analítica:

```text
source_row_number
source_file
ingestion_timestamp
torque_applicable
torque_spec_status
dq_is_valid
dq_error_codes
```

El resultado final es un contrato Silver de 31 columnas.
