# Serving en PostgreSQL

Esquema: `analytics`.

Objetos:

- `dim_process`: dimensión de proceso TU + PS + STEP sensible a la configuración.
- `dim_date`: calendario local de reporting.
- `fact_process_daily`: una fila por `date_key + process_key`.
- `etl_load_state`: estado incremental de particiones basado en checksum.
- `vw_process_daily_kpis`: vista BI aplanada con semántica de fábrica, línea/área, TU, PS y STEP.

La ruta normal `ensure_serving_ready()` detecta automáticamente un esquema `analytics` antiguo e incompatible, lo recrea una vez y luego continúa con cargas incrementales e idempotentes.


## Serving unificado batch + streaming

Los eventos streaming válidos se publican en `analytics.streaming_events` mediante Spark Structured Streaming. La tabla utiliza `result_id` y la posición Kafka como claves de idempotencia.

La vista `analytics.vw_process_daily_kpis` combina:

```text
Gold batch -> analytics.fact_process_daily
Streaming  -> analytics.streaming_events
```

y mantiene el mismo contrato consumido por Power BI. Los registros enviados a Quarantine no llegan a la capa de serving.
