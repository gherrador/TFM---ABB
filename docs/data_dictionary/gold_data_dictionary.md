# Diccionario de datos Gold

## `dim_process`

Granularidad: un proceso analítico sensible a configuración:

```text
tightening_unit_id + process_step_id + process_step_name + substep_id + substep_type
```

| Campo | Significado |
|---|---|
| `process_key` | Clave sustituta determinista SHA-256 para la granularidad de proceso. |
| `factory_id`, `factory_name` | Contexto de fábrica/sitio. |
| `line_id`, `line_name` | Contexto de línea. |
| `work_area_id`, `work_area_name` | Contexto de área de trabajo. |
| `tightening_unit_id`, `tightening_unit_name` | Contexto TU/estación. |
| `process_step_id`, `process_step_number`, `process_step_name` | Identidad y nombre del PS. |
| `substep_id`, `substep_number`, `substep_type` | Configuración STEP dentro del PS. |
| `tool_id_primary`, `tool_id_variant_count` | ID de herramienta más frecuente y cantidad de variantes observadas. |
| `tool_number_primary`, `tool_number_variant_count` | Número de herramienta principal y cantidad de variantes. |
| `tool_serial_number_primary`, `tool_serial_number_variant_count` | Serial físico principal y cantidad de variantes. |
| `target_torque`, `min_torque`, `max_torque` | Especificación homogénea de torque para eventos aplicables. |

## `dim_date`

Granularidad: una fila por fecha de calendario local de planta. Incluye claves y atributos de año, mes y día utilizados para reporting.

## `fact_process_daily`

Granularidad:

```text
date_key + process_key
```

Métricas principales:

- `event_count`;
- `torque_applicable_count`;
- `torque_not_applicable_count`;
- `torque_in_spec_count`;
- `torque_out_of_spec_count`;
- tasas de aplicabilidad y conformidad;
- estadísticas de torque;
- conteo de archivos fuente y trazabilidad temporal.

Los estados OK/NOK del sistema fuente no se agregan como KPI de calidad. La conformidad procede de la clasificación independiente realizada en Silver.
