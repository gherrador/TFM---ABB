# Spark Structured Streaming

## Objetivo

Apache Spark Structured Streaming implementa la capa de procesamiento en tiempo casi real del TFM.

```text
Kafka
  ↓
Decodificación Avro de Confluent
  ↓
Spark Structured Streaming
  ↓
Validación de calidad de datos
  ↓
Clasificación independiente de torque
  ↓
Streaming Silver / Quarantine
```

Implementación:

```text
src/processing/spark_streaming.py
```

Spark se inicia automáticamente mediante Docker Compose; ya no es necesario lanzar manualmente `spark-submit` con `docker exec`.

---

## Arranque automático

```powershell
docker compose up -d
docker compose ps
```

El contenedor `tfm-spark` espera a que Kafka y Schema Registry estén saludables y a que `kafka-init` complete la inicialización del topic.

Logs acotados:

```powershell
docker logs tfm-spark --tail 120
```

---

## Dependencias de Spark

La imagen utiliza Spark 3.5.9, Scala 2.12 y Java 17. Los conectores se resuelven con:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9
org.apache.spark:spark-avro_2.12:3.5.9
```

El primer arranque limpio requiere Internet para descargar estos artefactos desde repositorios Maven.

---

## Fuente Kafka

Spark se suscribe a:

```text
torque-events-raw
```

Bootstrap interno por defecto:

```text
kafka:29092
```

La configuración inicial utiliza `startingOffsets=earliest`; una vez creados los checkpoints, el progreso persistido determina el punto real de recuperación.

---

## Formato Avro de Confluent

El valor Kafka contiene:

```text
1 byte   magic byte
4 bytes  schema id
N bytes  carga útil Avro
```

Spark elimina los primeros cinco bytes antes de invocar `from_avro`.

También conserva metadatos de Kafka:

```text
kafka_key
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
```

---

## Reglas DQ de streaming

Un registro se considera inválido si se cumple cualquiera de estas reglas:

| Código | Condición |
|---|---|
| `DQ_MISSING_RESULT_ID` | `result_id` nulo o vacío. |
| `DQ_MISSING_EVENT_TIMESTAMP` | `event_timestamp` nulo. |
| `DQ_MISSING_TIGHTENING_UNIT` | `tightening_unit_id` nulo o vacío. |
| `DQ_MISSING_PROCESS_STEP` | `process_step_id` nulo o vacío. |
| `DQ_MISSING_SUBSTEP` | `substep_id` nulo o vacío. |
| `DQ_INVALID_TORQUE_LIMITS` | `min_torque > max_torque` cuando ambos límites existen. |

Los códigos se almacenan en `dq_error_codes` y `dq_is_valid` solo es verdadero si no existen errores.

---

## Aplicabilidad y clasificación de torque

El torque es aplicable cuando target, límites y torque aplicado están presentes, `max_torque > min_torque` y el target es distinto de cero.

Clasificación:

```text
DQ inválido                    → INVALID_DQ
DQ válido y torque no aplicable → NOT_APPLICABLE
min <= aplicado <= max         → IN_SPEC
aplicable fuera de límites     → OUT_OF_SPEC
```

Principio central:

```text
OUT_OF_SPEC → Silver
INVALID_DQ  → Quarantine
```

---

## Salidas y checkpoints

```text
data/streaming/silver/
data/streaming/quarantine/
data/streaming/checkpoints/
```

Se utilizan checkpoints independientes para Silver y Quarantine y un trigger micro-batch de cinco segundos por defecto.

---

## Recuperación validada

Prueba realizada:

```text
Silver antes de detener Spark: 1050
Eventos generados con Spark detenido: 50
Silver durante la detención: 1050
Silver tras reiniciar: 1100
Quarantine: 6
```

Kafka retuvo los eventos pendientes y Spark retomó desde el checkpoint sin duplicar los registros ya procesados.

---

## Inspección manual

```powershell
docker exec -it tfm-spark /opt/spark/bin/pyspark
```

```python
silver = spark.read.parquet('/opt/project/data/streaming/silver')
quarantine = spark.read.parquet('/opt/project/data/streaming/quarantine')

print('SILVER =', silver.count())
print('QUARANTINE =', quarantine.count())

silver.groupBy('torque_spec_status').count().show()
quarantine.groupBy('dq_error_codes').count().show(truncate=False)
```

No se deben eliminar checkpoints durante una prueba normal de reinicio, ya que hacerlo modifica la semántica de recuperación y puede provocar reprocesamiento desde `earliest`.


## Publicación en PostgreSQL y dashboard

Además de persistir Streaming Silver en Parquet, Spark publica cada micro-batch válido en `analytics.streaming_events` mediante JDBC. La carga utiliza una tabla de staging y `ON CONFLICT DO NOTHING` para mantener idempotencia por `result_id` y por posición Kafka.

PostgreSQL agrega esos eventos a la misma estructura lógica consumida por Power BI mediante `analytics.vw_process_daily_kpis`. De esta forma, el flujo demostrable es:

```text
Histórico batch -> PostgreSQL -> Power BI
Nuevos eventos -> Kafka -> Spark -> PostgreSQL -> Actualizar Power BI
```

Los eventos `INVALID_DQ` permanecen únicamente en Quarantine y no se publican en PostgreSQL.
