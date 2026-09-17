# Documentación técnica

Esta carpeta contiene la documentación técnica final del TFM. Los documentos históricos de fases intermedias, checklists de refactorización y versiones obsoletas fueron retirados para que el repositorio describa únicamente la arquitectura implementada y validada.

## Arquitectura

- `architecture/ARCHITECTURE.md`: arquitectura completa batch, streaming, serving y observabilidad.
- `architecture/LAYER_OWNERSHIP.md`: responsabilidad de cada capa de datos.
- `architecture/GOLD_LAYER.md`: modelo dimensional Gold y granularidad analítica.
- `architecture/GOLD_HANDOFF.md`: contrato de traspaso entre Silver y Gold.
- `architecture/POSTGRES_INCREMENTAL_SERVING.md`: diseño de carga incremental e idempotente hacia PostgreSQL.

## Diccionarios de datos

- `data_dictionary/Bronze_Silver_Data_Dictionary_v4.xlsx`: diccionario final de campos Bronze/Silver.
- `data_dictionary/bronze_projection.md`: proyección de campos Bronze hacia Silver.
- `data_dictionary/silver_data_dictionary.md`: contrato final de Silver.
- `data_dictionary/gold_data_dictionary.md`: contrato final de Gold.

## Decisiones de diseño

- `decisions/FINAL_SILVER_AUDIT.md`: auditoría semántica final de Silver.
- `decisions/MIGRATION_NOTES.md`: notas de la migración semántica Silver/ Gold.

## Serving

- `serving/POSTGRESQL.md`: capa de serving relacional.
- `serving/POWER_BI.md`: contrato de consumo desde Power BI.

## Streaming

- `streaming/kafka_avro_producer.md`: simulador, productor Kafka, Avro y Schema Registry.
- `streaming/spark_structured_streaming.md`: procesamiento con Spark Structured Streaming, reglas DQ y routing.
- `streaming/streaming_validation_evidence.md`: evidencias de validación, recuperación por checkpoints y prueba extremo a extremo.
- `streaming/control_center_observability.md`: telemetría Kafka, Prometheus, Alertmanager y Control Center.
