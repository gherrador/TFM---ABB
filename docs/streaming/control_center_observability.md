# Observabilidad de Kafka con Control Center

## Objetivo

La plataforma local incorpora observabilidad del broker Kafka mediante Confluent Control Center Next Gen, Prometheus y Alertmanager.

Arquitectura:

```text
Kafka broker
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

Este flujo es independiente del procesamiento de eventos de negocio.

---

## Componentes

```text
Kafka / Confluent Platform: 8.3.1
Control Center Next Gen:    2.6.0
Prometheus:                 imagen Confluent 2.6.0
Alertmanager:               imagen Confluent 2.6.0
```

Endpoints locales:

```text
Control Center: http://localhost:9021
Prometheus:     http://localhost:9090
Alertmanager:   http://localhost:9093
```

---

## Limitaciones de la topología local

El TFM utiliza un único broker Kafka. Por ello, los topics internos que lo requieren utilizan factor de replicación 1. Esta configuración es intencional para la prueba de concepto y no constituye un diseño productivo de alta disponibilidad.

Control Center puede mostrar una alerta relacionada con distribución de disco desigual. Con un solo broker dicha advertencia es esperable y no indica un fallo del pipeline.

---

## Arranque

```powershell
docker compose up -d
docker compose ps
```

Servicios relevantes:

```text
tfm-kafka
tfm-schema-registry
tfm-prometheus
tfm-alertmanager
tfm-control-center
tfm-spark
```

`tfm-kafka-init` es un contenedor de ejecución única y puede finalizar correctamente con `Exited (0)`.

---

## Configuración de telemetría Kafka

Las propiedades exactas se versionan en:

```text
docker/kafka/telemetry.properties
```

Entre las propiedades relevantes se encuentran:

```text
confluent.telemetry.exporter._c3.type=http
confluent.telemetry.exporter._c3.enabled=true
confluent.telemetry.exporter._c3.client.base.url=http://prometheus:9090/api/v1/otlp
confluent.telemetry.exporter._c3.client.compression=gzip
confluent.telemetry.remoteconfig._confluent.enabled=false
```

---

## Inyección de propiedades literales

La traducción automática de nombres de variables de entorno de las imágenes Confluent no representa de forma fiable los namespaces literales `._c3` y `._confluent`.

La solución final utiliza:

```text
docker/kafka/inject-telemetry.sh
```

Secuencia conceptual:

```text
Confluent configure
        ↓
inyectar propiedades exactas
        ↓
Confluent ensure
        ↓
Confluent launch
```

El script elimina variantes malformadas previas y añade las propiedades validadas.

---

## Configuración de Prometheus

Los archivos utilizados por la imagen de Confluent se encuentran en:

```text
docker/control-center/config/
```

Incluyen:

```text
prometheus-generated.yml
web-config-prom.yml
trigger_rules-generated.yml
alertmanager-generated.yml
web-config-am.yml
```

El entorno local no configura TLS ni autenticación HTTP básica.

---

## Promoción de atributos OTLP

Para que Control Center pueda asociar correctamente las métricas con cluster y broker, Prometheus promueve los atributos:

```yaml
otlp:
  promote_resource_attributes:
    - kafka.cluster.id
    - kafka.broker.id
    - kafka.version
    - host.hostname
```

Esto genera labels como:

```text
kafka_cluster_id
kafka_broker_id
kafka_version
host_hostname
```

---

## Evidencia validada

Se observó la métrica:

```text
io_confluent_telemetry_http_exporter_items_succeeded
```

con `exporter_name="_c3"`, confirmando exportaciones correctas.

También se validaron métricas del broker con atributos promovidos, entre ellas:

```text
io_confluent_kafka_server_replica_manager_partition_count
```

Control Center detectó finalmente un broker, topics, particiones y throughput de producción/consumo.

Los conteos exactos de topics y particiones son evidencia del entorno validado, no requisitos fijos de la arquitectura.

---

## Comprobaciones útiles

```powershell
Test-NetConnection localhost -Port 9090
Test-NetConnection localhost -Port 9093
Test-NetConnection localhost -Port 9021
```

Logs acotados:

```powershell
docker logs tfm-prometheus --tail 80
docker logs tfm-alertmanager --tail 80
docker logs tfm-control-center --tail 120
docker logs tfm-kafka --tail 120
```

Verificar propiedades efectivas:

```powershell
docker exec tfm-kafka bash -lc "grep -E '^confluent\.telemetry\.(exporter\._c3|remoteconfig\._confluent)\.' /etc/kafka/kafka.properties"
```

---

## Persistencia

Docker utiliza volúmenes para Kafka, PostgreSQL, Prometheus, Alertmanager y Control Center. No se debe utilizar `docker compose down -v` salvo que se busque deliberadamente un reset completo, ya que `-v` elimina los volúmenes nombrados.

---

## Alcance

La configuración demuestra observabilidad de Kafka en un entorno local de una sola instancia. Un entorno productivo requeriría múltiples brokers, alta disponibilidad, autenticación, TLS, autorización, políticas de alerta y planificación de capacidad.
