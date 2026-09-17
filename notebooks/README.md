# Notebooks de validación

Estos notebooks proporcionan exploración y evidencias de validación para el camino batch histórico. Complementan la suite automatizada de tests y no constituyen el camino de ejecución productivo.

## 01 — Profiling de datos

`01_data_profiling.ipynb`

Valida la estructura de Bronze y explora la jerarquía industrial:

```text
FCT → LIN / WKA → TU → PS → STEP → RES
```

También analiza identificadores PS/STEP reutilizados y sustenta la identidad de proceso sensible a la configuración utilizada posteriormente.

## 02 — Validación Bronze → Silver

`02_validate_bronze_to_silver.ipynb`

Valida el contrato Silver:

- proyección de los 24 campos de negocio;
- nombres y tipos canónicos;
- timestamps UTC;
- trazabilidad;
- anotaciones DQ;
- aplicabilidad de torque;
- clasificación independiente de conformidad.

## 03 — Validación Silver → Gold

`03_validate_silver_to_gold.ipynb`

Valida:

- unicidad de `process_key`;
- granularidad de la tabla de hechos diaria;
- campos requeridos de la jerarquía;
- reconciliación de eventos Gold con Silver;
- denominadores y tasas de KPIs de torque.

## 04 — Validación Gold → PostgreSQL

`04_validate_gold_to_postgres.ipynb`

Valida el traspaso hacia PostgreSQL, la reconciliación con los datasets Gold y el comportamiento incremental/idempotente de la capa de serving utilizada por Power BI.

## Rol dentro del TFM

Los notebooks son artefactos de análisis y validación. Los caminos reproducibles de ejecución se encuentran en los módulos Python bajo `src/`, Docker Compose y la suite automatizada bajo `test/`.

La suite final contiene 24 tests superados; los notebooks aportan evidencia adicional e inspeccionable de las transformaciones batch y contratos de serving.
