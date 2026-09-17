# TFM - Plataforma de Ingeniería de Datos para Eventos Industriales de Apriete

Proyecto reproducible de Ingeniería de Datos para el procesamiento histórico y en tiempo casi real de eventos industriales de apriete.

La solución combina un pipeline batch basado en una arquitectura Medallion con un flujo streaming orientado a eventos. Ambos caminos convergen en PostgreSQL, que actúa como capa común de serving para Power BI. La plataforma se ejecuta localmente mediante Docker Compose.

## Objetivos

- Ingerir y transformar históricos de producción.
- Preservar los archivos de origen en Bronze.
- Aplicar reglas de calidad de datos y conservar los registros inválidos en Quarantine.
- Diferenciar la validez del dato de la conformidad industrial del torque.
- Construir las capas Silver y Gold.
- Servir dimensiones, hechos y eventos streaming mediante PostgreSQL.
- Consumir KPIs desde Power BI.
- Simular y procesar eventos mediante Avro, Schema Registry, Kafka y Spark Structured Streaming.
- Validar recuperación mediante checkpoints e idempotencia práctica.
- Proporcionar observabilidad del plano Kafka mediante Prometheus y Confluent Control Center.

La migración a servicios cloud se considera una posible evolución futura y no forma parte del alcance implementado.

## Distribución y confidencialidad

El proyecto se distribuye en dos modalidades:

- **Entrega académica privada:** incluye los archivos históricos Bronze, los outputs procesados y el dashboard de Power BI necesarios para reproducir y verificar los resultados del TFM.
- **Repositorio público:** incluye código, pruebas, infraestructura, contratos, notebooks y documentación, pero excluye los datos industriales, los outputs derivados, las credenciales y los artefactos de Power BI que puedan incorporar datos en modo Import.

Los datos utilizados contienen información técnica de proceso industrial. Su distribución debe respetar las políticas de confidencialidad de la organización propietaria. Los archivos completos se proporcionan exclusivamente mediante el canal privado de evaluación académica.

El archivo `.env` no se publica por cuestiones de seguridad, en su lugar el archivo `.env.example` contiene únicamente valores locales de referencia.

## Inicio rápido

La ruta principal de reproduccion utiliza Docker Compose y los scripts incluidos en el proyecto.

### Windows PowerShell

Desde la raíz del proyecto:

```powershell
Copy-Item .env.example .env
.\scripts\setup.ps1
.\scripts\demo_streaming.ps1
```

`setup.ps1` realiza las siguientes operaciones:

1. Levanta la infraestructura.
2. Espera la disponibilidad de PostgreSQL y Kafka.
3. Ejecuta el pipeline batch.
4. Pública Gold en PostgreSQL.
5. Verifica el baseline histórico.

`demo_streaming.ps1` genera 100 eventos, espera su procesamiento por Spark Structured Streaming y comprueba su incorporación a PostgreSQL.

Resultado esperado con el paquete academico completo:

```text
Baseline histórico:     173.211 eventos
Eventos streaming:          100 eventos
Total unificado:        173.311 eventos
```

Después de la demostracion, actualizar Power BI mediante:

```text
Inicio -> Actualizar
```

### Linux o macOS

Se proporcionan scripts Bash equivalentes:

```bash
cp .env.example .env
chmod +x scripts/setup.sh scripts/demo_streaming.sh
./scripts/setup.sh
./scripts/demo_streaming.sh
```

La validación end-to-end principal del TFM se realizó en Windows mediante PowerShell. Los scripts Bash permanecen alineados con la misma configuración externalizada, pero deben verificarse en el sistema operativo objetivo antes de afirmar portabilidad multiplataforma validada.

## Arquitectura

La implementación es una arquitectura híbrida batch-streaming con características Lambda-like. No constituye una Lambda Architecture estricta: los dos caminos no calculan vistas equivalentes de la misma información ni mantienen serving layers independientes. Ambos convergen en una única capa relacional de serving.

### Pipeline batch

```text
CSV históricos
    |
    v
Bronze
    |
    v
Silver / Quarantine
    |
    v
Gold
    |
    v
PostgreSQL
    |
    v
Power BI
```

El flujo histórico sigue una arquitectura Medallion:

- **Bronze:** preserva los CSV de origen.
- **Silver:** aplica el contrato analítico canonico a nivel de evento.
- **Gold:** genera dimensiones, hechos y KPIs diarios.
- **Quarantine:** conserva registros que incumplen reglas de calidad.

### Pipeline streaming

```text
Simulador / Productor
    |
    v
Avro + Schema Registry
    |
    v
Kafka
    |
    v
Spark Structured Streaming
    |
    +--> Streaming Silver --> PostgreSQL --> Power BI
    |
    +--> Streaming Quarantine
```

Kafka actúa como backbone de eventos y desacopla productor y consumidor. Spark Structured Streaming procesa micro-batches, conserva offsets mediante checkpoints y publica los eventos válidos en Parquet y PostgreSQL.

### Observabilidad

```text
Kafka TelemetryReporter
    |
    v
Prometheus
    |
    v
Confluent Control Center
```

Prometheus también está integrado con Alertmanager. Alertmanager se encuentra desplegado, pero `trigger_rules-generated.yml` no contiene reglas activas. Por tanto, se presenta como una capacidad preparada y no como un sistema de alertado productivo completamente validado.

La documentación detallada se encuentra en:

```text
docs/architecture/ARCHITECTURE.md
```

## Jerarquía industrial

La jerarquía utilizada es:

```text
FCT
  |
  v
LIN / WKA
  |
  v
TU
  |
  v
PS
  |
  v
STEP
  |
  v
RES
```

- `FCT`: fábrica o sitio.
- `LIN`: línea.
- `WKA`: área de trabajo.
- `TU`: estación.
- `PS`: operación.
- `STEP`: paso.
- `RES`: resultado del evento.

`PS_Comment` se utiliza como nombre legible del Process Step. Los campos `CTRL_*` no se promueven a Silver porque no aportan segmentacion analítica relevante dentro del alcance actual.

## Capas de datos

### Bronze

Bronze conserva los archivos fuente y sus 75 columnas originales bajo:

```text
data/bronze/
```

Los tres archivos históricos de la entrega privada contienen:

| Fuente | Eventos |
|---|---:|
| Junio de 2025 | 40.500 |
| Septiembre de 2025 | 67.993 |
| Noviembre de 2025 | 64.718 |
| **Total** | **173.211** |

### Silver

Silver representa el contrato analítico canonico a nivel de evento. La transformacion:

- selecciona 24 campos fuente relevantes;
- normaliza nombres y tipos;
- normaliza timestamps a UTC;
- incorpora `source_file` y `source_row_number` para trazabilidad;
- aplica reglas de Data Quality;
- calcula aplicabilidad y conformidad del torque;
- produce 31 columnas finales.

`result_id` no es globalmente único en el histórico. La referencia técnica estable de la fila es:

```text
(source_file, source_row_number)
```

`RES_Report` se conserva como `source_result_status`, pero no se utiliza como verdad analítica para los KPIs de conformidad.

Los estados de torque son:

```text
IN_SPEC
OUT_OF_SPEC
NOT_APPLICABLE
```

Un evento `OUT_OF_SPEC` es un evento industrial válido y permanece en Silver.

### Data Quality y Quarantine

La solución diferencia entre no conformidad industrial y mala calidad del dato:

```text
OUT_OF_SPEC -> dato válido -> Silver
INVALID_DQ  -> dato inválido -> Quarantine
```

Los registros en Quarantine conservan los codigos de error para facilitar su analisis y eventual reprocesamiento.

En el histórico final:

```text
Silver:      173.211
Quarantine:        0
```

### Gold

Gold genera:

```text
dim_process
dim_date
fact_process_daily
```

La identidad lógica del proceso se forma con:

```text
tightening_unit_id
+ process_step_id
+ process_step_name
+ substep_id
+ substep_type
```

`process_key` se calcula de forma determinista mediante SHA-256. El grano de la tabla de hechos es:

```text
(date_key, process_key)
```

La tabla diaria incluye volumen, aplicabilidad, conteos `IN_SPEC` y `OUT_OF_SPEC`, tasas de conformidad, estadísticas descriptivas de torque y trazabilidad temporal.

Resultados Gold:

```text
dim_process:             311 filas
dim_date:                 64 filas
fact_process_daily:    5.545 filas
Eventos reconciliados: 173.211
```

## PostgreSQL y Power BI

PostgreSQL utiliza el esquema `analytics`.

Objetos principales:

```text
analytics.dim_process
analytics.dim_date
analytics.fact_process_daily
analytics.etl_load_state
analytics.streaming_events
analytics.vw_process_daily_kpis
```

La vista `analytics.vw_process_daily_kpis` combina los KPIs históricos y los agregados de eventos streaming mediante `UNION ALL`. Es el contrato principal consumido por Power BI.

La carga batch soporta incrementalidad e idempotencia práctica mediante:

- manifiesto Gold;
- hashes SHA-256;
- `analytics.etl_load_state`;
- claves primarias y `UPSERT`.

Streaming evita duplicados mediante:

- clave primaria `result_id`;
- restriccion única `(kafka_topic, kafka_partition, kafka_offset)`;
- `ON CONFLICT DO NOTHING`.

Estas garantias hacen la escritura tolerante a reintentos, sin afirmar semántica exactly-once extremo a extremo.

Power BI consume la vista en modo Import. El dashboard privado se encuentra en:

```text
dashboard/kpi_dashboard.pbix
```

El PBIX se incluye solamente en la entrega académica privada porque puede contener información derivada almacenada por el modelo Import. Para una distribución pública debe utilizarse una plantilla `.pbit` sin datos o una versión conectada a fuentes sintéticas.

## Streaming con Kafka y Spark

Componentes:

- Apache Kafka en modo KRaft.
- Confluent Schema Registry.
- Avro.
- Apache Spark Structured Streaming.
- Parquet.
- PostgreSQL.
- Docker Compose.

Topic principal:

```text
torque-events-raw
```

Configuración local:

```text
3 particiones
replication factor = 1
```

El factor de replicación 1 es intencional para una prueba de concepto con un único broker y no representa alta disponibilidad.

### Productor y contrato Avro

```text
src/iot/kafka_producer.py
src/iot/torque_event_simulator.py
src/schemas/torque_event.avsc
```

El productor registra y consulta el contrato mediante Schema Registry. Spark elimina la cabecera Confluent de cinco bytes y deserializa utilizando una copia local versionada del esquema Avro.

El simulador muestrea configuraciones históricas segun su frecuencia e incorpora una proporcion configurable de nuevas unidades de apriete sintéticas.

### Spark Structured Streaming

```text
src/processing/spark_streaming.py
```

Spark:

- consume Kafka;
- conserva topic, partición, offset y timestamp;
- decodifica Avro;
- ejecuta reglas de Data Quality;
- calcula aplicabilidad y conformidad;
- escribe Streaming Silver y Streaming Quarantine;
- publica eventos válidos en `analytics.streaming_events`;
- mantiene checkpoints persistentes.

Salidas:

```text
data/streaming/silver/
data/streaming/quarantine/
data/streaming/checkpoints/
```

Spark se inicia automáticamente mediante Docker Compose. No es necesario lanzar manualmente `spark-submit`.

### Recuperación mediante checkpoints

Se validó el siguiente comportamiento:

```text
Spark detenido
-> Kafka continua recibiendo eventos
-> los eventos quedan pendientes
-> Spark reinicia
-> retoma desde el checkpoint
```

En una prueba controlada, se produjeron 50 eventos con Spark detenido. Tras reiniciar el consumidor, Spark proceso exactamente el backlog pendiente sin repetir los registros ya persistidos.

### Validación de Data Quality streaming

El productor permite generar seis eventos Avro estructuralmente válidos pero semánticamente inválidos:

```powershell
docker compose --profile producer run --rm producer --inject-dq --interval 0.1
```

Las seis reglas ejercitadas son:

- resultado ausente;
- timestamp ausente;
- unidad de apriete ausente;
- Process Step ausente;
- substep ausente;
- límites de torque incoherentes.

Los seis eventos se envian a Streaming Quarantine sin contaminar Streaming Silver.

La campaña de validación ampliada alcanzó 1.200 eventos válidos y 12 registros en Quarantine. La reconstrucción limpia conservada en el paquete final contiene 100 eventos válidos y 0 registros en Streaming Quarantine. Ambos resultados corresponden a ejecuciones diferentes y se documentan en:

```text
docs/streaming/streaming_validation_evidence.md
```

## Observabilidad

Endpoints locales:

```text
Control Center:  http://localhost:9021
Prometheus:      http://localhost:9090
Alertmanager:    http://localhost:9093
Schema Registry: http://localhost:8081
```

Control Center permite inspeccionar:

- estado del broker;
- topics;
- particiones;
- actividad y throughput;
- métricas operativas de Kafka.

Limitaciones:

- el despliegue utiliza un único broker;
- no existe alta disponibilidad;
- Alertmanager no dispone de reglas de disparo activas;
- no se implementan logs centralizados ni trazas distribuidas;
- la observabilidad se concentra en el plano Kafka.

## Requisitos

### Ruta Docker-first

Para ejecutar `setup.ps1` y `demo_streaming.ps1`:

- Docker Desktop o Docker Engine.
- Docker Compose.
- Acceso a Internet durante el primer arranque.
- Power BI Desktop, únicamente para abrir y actualizar el dashboard.

El primer arranque requiere Internet para descargar imagenes y resolver mediante Maven:

```text
spark-sql-kafka
spark-avro
PostgreSQL JDBC
```

Python y Poetry no son necesarios para la ruta Docker-first.

### Desarrollo y pruebas desde el host

Para ejecutar modulos o pruebas directamente desde el host:

- Python 3.12 o 3.13.
- Poetry.

Instalacion:

```powershell
poetry install
```

## Configuración

La configuración se externaliza mediante:

```text
.env
.env.example
src/config/settings.py
```

Crear la configuración local:

```powershell
Copy-Item .env.example .env
```

O en Bash:

```bash
cp .env.example .env
```

Los scripts consultan `POSTGRES_USER` y `POSTGRES_DB` dentro del contenedor y no dependen de credenciales codificadas directamente.

El archivo `.env` está excluido mediante `.gitignore` y no debe incluirse en el repositorio público ni en la entrega académica.

## Ejecución manual

Los scripts constituyen la ruta recomendada. Los siguientes comandos permiten ejecutar cada operación de forma individual.

### Levantar la infraestructura

```powershell
docker compose up -d
docker compose ps
```

Servicios principales:

```text
tfm-postgres
tfm-kafka
tfm-schema-registry
tfm-spark
tfm-prometheus
tfm-alertmanager
tfm-control-center
```

`tfm-kafka-init` es un servicio one-shot y debe finalizar con `Exited (0)` después de crear o verificar el topic.

### Ejecutar batch

```powershell
docker compose --profile batch run --rm pipeline
```

Flujo:

```text
Bronze -> Silver/Quarantine -> Gold -> PostgreSQL
```

### Generar eventos streaming

Mediante el script de demostracion:

```powershell
.\scripts\demo_streaming.ps1
```

Cantidad personalizada:

```powershell
.\scripts\demo_streaming.ps1 -Count 250 -Interval 0.05
```

Mediante Docker Compose:

```powershell
docker compose --profile producer run --rm producer --count 100 --interval 0.1
```

Modo continuo:

```powershell
docker compose --profile producer run --rm producer
```

### Generar eventos inválidos

```powershell
docker compose --profile producer run --rm producer --inject-dq --interval 0.1
```

### Detener la plataforma

```powershell
docker compose down
```

No utilizar `docker compose down -v` salvo que se desee eliminar deliberadamente los volumenes persistentes.

## Pruebas y validaciones

### Suite automatizada

```powershell
poetry run pytest -q
```

Resultado validado:

```text
24 passed
```

La suite cubre:

- simulador de eventos;
- reglas de Data Quality;
- contrato Silver;
- contrato Gold;
- reconciliación;
- contrato de serving PostgreSQL;
- configuración y normalizacion.

La integracion completa Kafka-Spark-PostgreSQL se valida mediante ejercicios end-to-end controlados y no mediante una única prueba pytest automatizada.

### Validar el proyecto Python

```powershell
poetry check
```

### Validar Docker Compose

```powershell
docker compose config
```

### Logs

```powershell
docker logs tfm-spark --tail 120
docker logs tfm-kafka --tail 120
docker logs tfm-control-center --tail 120
```

## Resultados finales

### Histórico batch

| Métrica | Resultado |
|---|---:|
| Eventos Silver | 173.211 |
| Columnas Silver | 31 |
| Quarantine histórica | 0 |
| Procesos en `dim_process` | 311 |
| Fechas en `dim_date` | 64 |
| Filas en `fact_process_daily` | 5.545 |
| Eventos con torque aplicable | 169.565 |
| `IN_SPEC` | 165.325 |
| `OUT_OF_SPEC` | 4.240 |
| `NOT_APPLICABLE` | 3.646 |
| Tasa `IN_SPEC` sobre aplicables | 97,50 % |
| Tasa `OUT_OF_SPEC` sobre aplicables | 2,50 % |

### Reconstrucción final batch-streaming

```text
173.211 eventos batch
+   100 eventos streaming
=173.311 eventos unificados
```

Los 100 eventos streaming conservados se distribuyen en:

```text
IN_SPEC:         96
OUT_OF_SPEC:      1
NOT_APPLICABLE:   3
```

## Persistencia y reinicios

Docker utiliza volumenes nombrados para conservar:

- PostgreSQL;
- Kafka;
- Prometheus;
- Alertmanager;
- Control Center.

Reinicio normal:

```powershell
docker compose down
docker compose up -d
```

Los datasets streaming y sus checkpoints se almacenan bajo `data/streaming/` y se montan desde el host.

## Inspección manual

Entrar a PySpark:

```powershell
docker exec -it tfm-spark /opt/spark/bin/pyspark
```

Ejemplo:

```python
silver = spark.read.parquet("/opt/project/data/streaming/silver")
quarantine = spark.read.parquet("/opt/project/data/streaming/quarantine")

print("SILVER =", silver.count())
print("QUARANTINE =", quarantine.count())

silver.groupBy("torque_spec_status").count().show()
quarantine.groupBy("dq_error_codes").count().show(truncate=False)
```

## Estructura del repositorio

```text
.
|-- dashboard/
|-- data/
|-- docker/
|-- docs/
|-- notebooks/
|-- scripts/
|-- src/
|-- test/
|-- .dockerignore
|-- .env.example
|-- .gitignore
|-- docker-compose.yml
|-- poetry.lock
|-- pyproject.toml
`-- README.md
```

`src/` contiene:

```text
config/
data_quality/
iot/
metadata/
pipelines/
processing/
schemas/
serving/
transformation/
```

Documentación principal:

```text
docs/README.md
docs/architecture/ARCHITECTURE.md
docs/architecture/GOLD_LAYER.md
docs/architecture/POSTGRES_INCREMENTAL_SERVING.md
docs/data_dictionary/
docs/serving/POSTGRESQL.md
docs/serving/POWER_BI.md
docs/streaming/kafka_avro_producer.md
docs/streaming/spark_structured_streaming.md
docs/streaming/streaming_validation_evidence.md
docs/streaming/control_center_observability.md
```

Los notebooks cubren profiling, validación Bronze-Silver, validación Silver-Gold y comprobación Gold-PostgreSQL.

## Contenido de cada modalidad

### Entrega académica privada

Incluye:

```text
data/bronze/*.csv
data/silver/
data/gold/
data/quarantine/
data/streaming/
dashboard/kpi_dashboard.pbix
```

También incluye código, pruebas, configuración de infraestructura, notebooks y documentación. No incluye `.env`.

### Repositorio público

Excluye:

```text
.env
data/bronze/*.csv
data/silver/
data/gold/
data/quarantine/
data/streaming/silver/
data/streaming/quarantine/
data/streaming/checkpoints/
dashboard/*.pbix
```

Para ejecutar el pipeline batch desde el repositorio público es necesario proporcionar fuentes compatibles y autorizadas bajo `data/bronze/`.

Si un archivo ya fue incorporado al historial de Git, agregarlo a `.gitignore` no lo elimina del historial. Debe retirarse del indice antes de publicar el repositorio.

## Alcance y limitaciones

- La plataforma se ejecuta en un único equipo.
- Kafka utiliza un broker y replication factor 1.
- Los eventos streaming proceden de un simulador, no de PLC, herramientas de apriete o MES reales.
- Power BI funciona en modo Import y requiere actualizacion.
- Batch y streaming aplican principios compatibles, pero sus reglas DQ están implementadas por separado.
- No se implementan modelos predictivos por ausencia de etiquetas fiables de fallo o mantenimiento.
- No se implementa cloud, Kubernetes, CI/CD productivo, alta disponibilidad ni pruebas de carga.
- Kafka no utiliza TLS/SASL y PostgreSQL no utiliza TLS en el entorno local.
- No existe gestión empresarial de secretos.
- No se han definido benchmarks formales de latencia o throughput.
- Alertmanager está desplegado, pero no existen reglas activas de disparo.
- La validación end-to-end principal se realizó mediante PowerShell en Windows.


## Estado final de validación

```text
Integridad de fuentes históricas           OK
Bronze -> Silver / Quarantine              OK
Silver -> Gold                             OK
Reconciliación de 173.211 eventos          OK
PostgreSQL serving                         OK
Power BI                                   OK
Kafka + Schema Registry                    OK
Spark Structured Streaming                 OK
Streaming Silver / Quarantine              OK
Recuperacion mediante checkpoints          OK
100 eventos streaming                      OK
Total unificado de 173.311 eventos         OK
Control Center / Prometheus                OK
24 pruebas automatizadas                   OK
```

El proyecto constituye una prueba de concepto local, contenerizada y reproducible de Ingeniería de Datos para el procesamiento batch y streaming de eventos industriales de apriete.
