# Responsabilidad de las capas

- **Bronze**: copia inmutable del origen; preserva las 75 columnas fuente.
- **Silver**: proyección de 24 campos de negocio, nombres y tipos canónicos, timestamps UTC, trazabilidad técnica, DQ, Quarantine y clasificación de torque a nivel de evento.
- **Gold**: modelo dimensional de proceso/fecha y KPIs/estadísticas diarios de torque.
- **PostgreSQL**: esquema de serving y vista para BI.
- **Power BI**: consumo interactivo; no reimplementa reglas de negocio principales más allá de medidas sensibles al contexto calculadas sobre los conteos de Gold.
