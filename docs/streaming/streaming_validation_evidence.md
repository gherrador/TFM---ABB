# Evidencias de validación del streaming

## Objetivo

Validar el flujo local completo:

```text
Simulador / Productor
        ↓
Avro + Schema Registry
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Calidad de datos + clasificación de torque
        ↓
Streaming Silver / Quarantine
```

La validación cubre simulación realista, publicación Kafka, serialización Avro, decodificación Spark, routing DQ, clasificación independiente de torque, recuperación por checkpoints y reconciliación de registros.

---

## 1. Validación del simulador ponderado

Una ejecución controlada de 1.000 eventos produjo:

| Clasificación | Filas | Porcentaje |
|---|---:|---:|
| `IN_SPEC` | 963 | 96,3 % |
| `NOT_APPLICABLE` | 14 | 1,4 % |
| `OUT_OF_SPEC` | 23 | 2,3 % |
| **Total** | **1.000** | **100 %** |

La tasa OOS observada fue del 2,3 %, coherente con la probabilidad configurada y con el muestreo ponderado por frecuencia histórica.

---

## 2. Conformidad frente a calidad de datos

```text
Evento válido dentro de límites   → Silver → IN_SPEC
Evento válido fuera de límites    → Silver → OUT_OF_SPEC
Evento válido no aplicable        → Silver → NOT_APPLICABLE
Registro inválido por DQ          → Quarantine → INVALID_DQ
```

`OUT_OF_SPEC` no representa un registro malformado, sino información industrial válida.

---

## 3. Prueba controlada de DQ

Comando:

```powershell
poetry run python -m iot.kafka_producer --inject-dq --interval 0.1
```

Se publicaron seis eventos Avro válidos pero semánticamente inválidos, uno para cada regla:

| Código DQ | Cantidad | Resultado esperado |
|---|---:|---|
| `DQ_MISSING_RESULT_ID` | 1 | Quarantine |
| `DQ_MISSING_EVENT_TIMESTAMP` | 1 | Quarantine |
| `DQ_MISSING_TIGHTENING_UNIT` | 1 | Quarantine |
| `DQ_MISSING_PROCESS_STEP` | 1 | Quarantine |
| `DQ_MISSING_SUBSTEP` | 1 | Quarantine |
| `DQ_INVALID_TORQUE_LIMITS` | 1 | Quarantine |

Resultado original:

```text
Streaming Silver      = 1000
Streaming Quarantine  = 6
```

Los seis registros presentaron `dq_is_valid=false` y `torque_spec_status=INVALID_DQ`.

---

## 4. Recuperación mediante checkpoints

Baseline:

```text
Silver      = 1050
Quarantine  = 6
```

Procedimiento:

1. detener Spark Structured Streaming;
2. mantener Kafka operativo;
3. publicar 50 eventos válidos;
4. confirmar que Silver no cambia mientras Spark está detenido;
5. reiniciar Spark conservando checkpoints;
6. volver a contar los datasets.

Resultado:

```text
Silver mientras Spark estaba detenido = 1050
Silver tras reiniciar                  = 1100
Quarantine                             = 6
```

Los 50 eventos pendientes fueron procesados tras el reinicio sin duplicación de los registros anteriores.

---

## 5. Validación extremo a extremo final

Estado inicial:

```text
Silver      = 1100
Quarantine  = 6
```

### Paso A — 100 eventos válidos

```powershell
poetry run python -m iot.kafka_producer --count 100 --interval 0.1
```

Resultado:

```text
Silver      = 1200
Quarantine  = 6
```

Reconciliación:

```text
1200 - 1100 = 100 nuevas filas válidas
6 - 6       = 0 nuevos registros inválidos
```

### Paso B — seis eventos DQ inválidos

```powershell
poetry run python -m iot.kafka_producer --inject-dq --interval 0.1
```

Resultado:

```text
Silver      = 1200
Quarantine  = 12
```

Reconciliación:

```text
1200 - 1200 = 0 nuevas filas Silver
12 - 6      = 6 nuevas filas Quarantine
```

---

## 6. Resultado final

```text
Kafka: persistencia de eventos                 OK
Schema Registry / Avro                         OK
Consumo Spark Structured Streaming             OK
Decodificación Avro de Confluent               OK
Validación semántica DQ                        OK
Routing Silver / Quarantine                    OK
Clasificación independiente de torque          OK
OUT_OF_SPEC conservado como dato válido        OK
Recuperación mediante checkpoints              OK
Reconciliación de eventos controlados          OK
Arranque automático de Spark                   OK
```

La validación confirma de extremo a extremo la separación entre conformidad industrial y validez del dato.
