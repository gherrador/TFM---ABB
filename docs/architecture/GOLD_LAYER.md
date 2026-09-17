# Capa Gold

Gold es un modelo dimensional orientado a serving construido a partir de Silver.

Contiene:

- `dim_process`: dimensión de proceso sensible a la configuración TU + PS + STEP.
- `dim_date`: calendario local de planta.
- `fact_process_daily`: agregados diarios de eventos y conformidad de torque.

Gold no reproduce diagnósticos de origen que no se utilizan analíticamente. El estado OK/NOK del sistema fuente se conserva únicamente en Silver como `source_result_status`; los KPIs de conformidad de Gold se calculan de forma independiente a partir del torque medido y los límites de especificación.
