# Notas de migración — Silver / Gold

Esta migración constituye una reducción controlada del contrato analítico a partir del profiling de una fuente Bronze de 75 columnas.

## Campos eliminados respecto de Silver 3.0

Entre los campos fuente que dejaron de promoverse se encuentran:

- `RES_ResultNumber`, `RES_StepNumber`, `RES_ErrorCode`, `RES_StopSource`, `RES_VIN`, `RES_CycleOKCount`;
- `TU_Number`, `TU_Comment`;
- campos de controlador, versiones, ángulo, torque-rate, corriente y segundo transductor que no forman parte del caso de uso analítico final.

## Campos de resultado retenidos

```text
RES_ID          → result_id
RES_Report      → source_result_status
RES_DateTime    → event_timestamp
RES_FinalTorque → applied_torque
```

Los identificadores de herramienta se conservan para análisis futuros de equipamiento.

## Cambio semántico principal

El estado `source_result_status` no se utiliza como KPI de conformidad. La clasificación analítica se calcula de forma independiente mediante torque aplicado y límites de especificación.

Gold elimina agregados basados en `RES_VIN` y estados fuente que no forman parte del contrato final, y adopta una identidad de proceso sensible a la configuración.
