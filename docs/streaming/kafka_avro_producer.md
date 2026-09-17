# Productor Kafka Avro y simulador de eventos de apriete

## Objetivo

La capa de entrada streaming simula eventos industriales de apriete y los publica en Kafka mediante serialización Avro y Confluent Schema Registry.

Flujo implementado:

```text
Catálogo de procesos Silver histórico
        ↓
Simulador de eventos de torque
        ↓
Serialización Avro + Schema Registry
        ↓
Topic Kafka: torque-events-raw
```

Implementación:

```text
src/iot/kafka_producer.py
src/iot/torque_event_simulator.py
src/schemas/torque_event.avsc
```

El simulador no modifica los datasets históricos. Silver se utiliza únicamente como catálogo de configuraciones reales de proceso.

---

## Muestreo histórico

El simulador construye un catálogo de configuraciones y conserva la frecuencia de ocurrencia observada en los datos históricos. Una configuración rara no recibe la misma probabilidad que otra utilizada miles de veces en producción.

Esto permite que las pruebas streaming mantengan una distribución más realista.

---

## Semántica de los eventos simulados

Cada evento incluye la configuración seleccionada y datos nuevos a nivel de evento:

- nuevo `result_id`;
- timestamp UTC actual;
- torque aplicado;
- estado fuente.

También pueden aparecer dos unidades de apriete simuladas persistentes:

```text
TU90-DEMO-1
TU91-DEMO-2
```

El simulador distingue entre:

```text
evento normal
evento fuera de especificación
evento con torque no aplicable
evento de nueva unidad simulada
```

Un evento `OUT_OF_SPEC` continúa siendo un registro analíticamente válido. No se envía a Quarantine por estar fuera de tolerancia.

---

## Configuración

Los parámetros se controlan mediante `.env`, `.env.example` y `src/config/settings.py`.

Variables principales:

```text
KAFKA_BOOTSTRAP_SERVERS
SCHEMA_REGISTRY_URL
KAFKA_RAW_TOPIC
KAFKA_SECURITY_PROTOCOL
EXISTING_TU_OOS_PROBABILITY
NEW_TU_EVENT_PROBABILITY
NEW_TU_OOS_PROBABILITY
STREAM_EVENT_INTERVAL_SECONDS
```

Por defecto, el productor ejecutado desde el host utiliza:

```text
Kafka:           localhost:9092
Schema Registry: http://localhost:8081
```

---

## Arranque de la plataforma

```powershell
docker compose up -d
docker compose ps
```

El topic `torque-events-raw` se crea automáticamente mediante el servicio de ejecución única `kafka-init` con tres particiones y factor de replicación 1.

---

## Publicar eventos

Ejemplo de 100 eventos:

```powershell
poetry run python -m iot.kafka_producer --count 100 --interval 0.1
```

Modo continuo:

```powershell
poetry run python -m iot.kafka_producer
```

Puede utilizarse `--seed` para obtener comportamiento reproducible del simulador.

---

## Inyección controlada de DQ

```powershell
poetry run python -m iot.kafka_producer --inject-dq --interval 0.1
```

Este modo publica exactamente seis eventos válidos para Avro pero inválidos semánticamente:

```text
result_id ausente
event_timestamp ausente
tightening_unit_id ausente
process_step_id ausente
substep_id ausente
límites de torque incoherentes
```

Resultado esperado:

```text
Avro válido + DQ válido   → Streaming Silver
Avro válido + DQ inválido → Streaming Quarantine
```

---

## Verificar Schema Registry

```powershell
Invoke-RestMethod http://localhost:8081/subjects
```

Después de publicar eventos debe existir el subject:

```text
torque-events-raw-value
```

---

## Consumer técnico de verificación

```powershell
poetry run python -m iot.kafka_avro_consumer --count 5 --from-beginning
```

Este consumer permite comprobar manualmente que los eventos fueron serializados, registrados y publicados correctamente.

---

## Fallo de Schema Registry

Si Schema Registry no está disponible, la serialización Avro falla antes de producir el mensaje en Kafka. En ese caso se debe comprobar el estado de Docker y el endpoint de Schema Registry; no es necesario modificar el código.

---

## Estado de validación

El productor y simulador fueron validados en la prueba extremo a extremo final. Se añadieron 100 eventos válidos sin incremento de Quarantine y posteriormente seis eventos DQ inválidos que fueron dirigidos exclusivamente a Quarantine.
