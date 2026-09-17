# Arquitectura del proyecto

## Visión general

Este TFM implementa una plataforma local y reproducible de Ingeniería de Datos para eventos industriales de apriete. La solución combina un pipeline batch histórico, un pipeline streaming en tiempo casi real, una capa de serving relacional, una capa de consumo en Power BI y observabilidad de Kafka.

La arquitectura está contenerizada mediante Docker Compose. La migración a servicios nube se considera una evolución futura y no forma parte de la implementación validada.

---

## 1. Arquitectura batch histórica

```text
Fuentes CSV históricas
        ↓
Bronze
        ↓
Silver
        ↓
Gold
        ↓
PostgreSQL analytics
        ↓
Power BI
```

### Bronze

Bronze preserva los datos originales sin reducción destructiva:

- mantiene las 75 columnas de origen;
- conserva los archivos históricos como referencia fuente;
- no aplica reglas analíticas ni elimina información.

### Silver

Silver constituye la capa canónica a nivel de evento y realiza:

- selección de 24 campos fuente relevantes;
- nombres y tipos canónicos;
- normalización de timestamps a UTC;
- trazabilidad técnica;
- validación de calidad de datos;
- routing de registros inválidos hacia Quarantine;
- cálculo independiente de aplicabilidad y conformidad del torque.

La jerarquía industrial retenida es:

```text
FCT → LIN/WKA → TU → PS → STEP → RES
```

Donde `PS_Comment` se utiliza como nombre legible de la operación o Process Step.

`RES_Report` se conserva como `source_result_status` únicamente como metadatos de origen. La conformidad del torque se calcula de forma independiente a partir del torque medido y los límites configurados.

### Gold

Gold es una capa dimensional orientada a informes y serving. Contiene:

```text
dim_process
dim_date
fact_process_daily
```

La identidad analítica del proceso es sensible a la configuración y se construye con:

```text
tightening_unit_id
+ process_step_id
+ process_step_name
+ substep_id
+ substep_type
```

Los identificadores de herramienta se mantienen como descriptores, pero no forman parte de la identidad lógica del proceso.

### PostgreSQL

Gold se publica en el esquema `analytics` de PostgreSQL. Los objetos principales son:

```text
analytics.dim_process
analytics.dim_date
analytics.fact_process_daily
analytics.etl_load_state
analytics.vw_process_daily_kpis
```

La carga soporta ejecución incremental e idempotente mediante checksums y estado de carga.

### Power BI

Power BI consume `analytics.vw_process_daily_kpis` en modo Import. Las reglas principales de calidad y conformidad se calculan aguas arriba para evitar duplicar lógica de negocio en la capa de visualización.

---

## 2. Arquitectura streaming

```text
Simulador / productor de eventos
        ↓
Serialización Avro
        ↓
Confluent Schema Registry
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Calidad de datos + clasificación de torque
        ↓
Streaming Silver / Quarantine
        ↓
PostgreSQL streaming_events
        ↓
Vista de serving unificada
        ↓
Power BI
```

### Productor

El productor local genera eventos realistas utilizando configuraciones muestreadas desde el histórico Silver y publica en:

```text
torque-events-raw
```

Puede generar eventos normales, eventos `OUT_OF_SPEC`, eventos sin torque aplicable, nuevas unidades de apriete simuladas y casos deterministas de DQ.

### Schema Registry

Confluent Schema Registry gestiona el contrato Avro definido en:

```text
src/schemas/torque_event.avsc
```

La validación de schema y la validación semántica de calidad de datos son responsabilidades distintas.

### Kafka

Kafka se ejecuta localmente en modo KRaft con un único broker. El topic principal dispone por defecto de tres particiones y factor de replicación 1. Esta configuración responde al alcance local de la prueba de concepto y no representa una topología productiva de alta disponibilidad.

### Spark Structured Streaming

Spark consume Kafka de forma continua y:

- decodifica el formato Avro de Confluent;
- conserva metadatos técnica de Kafka;
- evalúa reglas DQ;
- recalcula aplicabilidad de torque;
- clasifica la conformidad de forma independiente;
- escribe los registros válidos en Streaming Silver;
- envía registros DQ inválidos a Quarantine.

La distinción central es:

```text
OUT_OF_SPEC → evento industrial válido → Silver
INVALID_DQ  → registro de datos inválido → Quarantine
```

Las salidas se almacenan en:

```text
data/streaming/silver/
data/streaming/quarantine/
data/streaming/checkpoints/
```

Spark se inicia automáticamente al ejecutar:

```powershell
docker compose up -d
```

---

## 3. Estado y recuperación del streaming

Spark Structured Streaming utiliza checkpoints persistentes para conservar offsets y progreso de procesamiento. Se validó que, si Spark se detiene mientras Kafka continúa recibiendo mensajes, los eventos quedan pendientes y se procesan al reiniciar Spark desde el checkpoint existente, sin reprocesar los registros ya persistidos.

---

## 4. Observabilidad

La observabilidad del broker utiliza:

```text
Kafka
  ↓
Confluent TelemetryReporter
  ↓ OTLP/HTTP
Prometheus
  ↓
Control Center Next Gen

Prometheus
  ↓
Alertmanager
```

La observabilidad es independiente del flujo de datos de negocio y permite inspeccionar broker, topics, particiones, throughput y métricas operativas.

---

## 5. Orquestación con Docker Compose

Servicios permanentes:

```text
postgres
kafka
schema-registry
spark
prometheus
alertmanager
control-center
```

Servicio de ejecución única:

```text
kafka-init
```

El pipeline batch se ejecuta de forma explícita mediante el profile `batch`:

```powershell
docker compose --profile batch run --rm pipeline
```

De esta forma, iniciar la infraestructura no provoca automáticamente un reprocesamiento histórico.

---

## 6. Configuración y reproducibilidad

La configuración se externaliza mediante:

```text
.env
.env.example
src/config/settings.py
```

Las dependencias Python se controlan mediante:

```text
pyproject.toml
poetry.lock
```

La suite automatizada final contiene 24 tests y valida contratos DQ, Silver, Gold, serving y simulación.

El primer arranque limpio de Spark requiere acceso a Internet para resolver los conectores Kafka y Avro indicados mediante `--packages`.

---

## 7. Validación extremo a extremo final

La validación controlada final produjo:

```text
Estado inicial             1100 Silver /  6 Quarantine
+100 eventos válidos       1200 Silver /  6 Quarantine
+6 eventos DQ inválidos    1200 Silver / 12 Quarantine
```

La reconciliación demuestra que los 100 eventos válidos llegaron a Silver y los seis eventos deliberadamente inválidos fueron aislados en Quarantine.

---

## 8. Alcance y evolución futura

La implementación entregada es una prueba de concepto local y contenerizada. La arquitectura está desacoplada para permitir, en una evolución futura, migraciones hacia Kafka gestionado, Spark gestionado, almacenamiento nube y serving relacional gestionado. Estas posibilidades no se presentan como funcionalidades implementadas en el TFM.


## Convergencia batch y streaming en serving

La convergencia entre el histórico batch y los nuevos eventos streaming se realiza en PostgreSQL, sin mezclar físicamente las dos capas Silver. `analytics.fact_process_daily` conserva el histórico agregado generado por Gold, mientras que `analytics.streaming_events` conserva los eventos válidos recibidos por Kafka a granularidad de evento. La vista `analytics.vw_process_daily_kpis` presenta ambos orígenes con el mismo contrato de consumo para Power BI.

Esta decisión permite demostrar la evolución del sistema desde cargas batch hacia streaming manteniendo trazabilidad, aislamiento de Quarantine y un único dashboard.
