# Serving incremental en PostgreSQL

Los archivos fact de Gold se registran en `analytics.etl_load_state` mediante ruta relativa y checksum SHA-256. Las particiones sin cambios se omiten; las nuevas particiones se cargan mediante upsert y quedan registradas. Si una partición histórica ya cargada cambia, el proceso falla explícitamente y requiere un refresco completo.

Una migración semántica o de schema se gestiona automáticamente: si faltan columnas obligatorias de Gold, el esquema `analytics` anterior se elimina y recrea a partir del contrato SQL vigente. A continuación se cargan una vez todas las particiones Gold actuales.
