# Capa semántica Power BI — Gold

Fuente recomendada: `analytics.vw_process_daily_kpis` en modo **Import**.

Medidas KPI recomendadas; reemplazar `TABLE_NAME` por el nombre de la tabla importada:

```DAX
Total Events = SUM('TABLE_NAME'[event_count])

Torque Applicable Rate =
DIVIDE(
    SUM('TABLE_NAME'[torque_applicable_count]),
    SUM('TABLE_NAME'[event_count]),
    0
)

Torque In Spec Rate =
DIVIDE(
    SUM('TABLE_NAME'[torque_in_spec_count]),
    SUM('TABLE_NAME'[torque_applicable_count]),
    0
)

Torque Out Of Spec Rate =
DIVIDE(
    SUM('TABLE_NAME'[torque_out_of_spec_count]),
    SUM('TABLE_NAME'[torque_applicable_count]),
    0
)
```

## Dimensiones de análisis

La vista permite segmentar por:

- fábrica;
- línea;
- área de trabajo;
- unidad de apriete;
- Process Step;
- subpaso/configuración;
- fecha.

## Principio de diseño

Power BI consume resultados ya normalizados y agregados. Las reglas de calidad de datos y la clasificación principal de torque se implementan aguas arriba para mantener una única definición analítica.


## Incorporación de eventos streaming

El dashboard continúa consumiendo `analytics.vw_process_daily_kpis`. No es necesario cambiar la fuente: la vista fue ampliada en PostgreSQL para incluir tanto el histórico batch como los eventos streaming válidos.

Como el modelo utiliza Import, después de producir nuevos eventos Kafka se debe ejecutar **Actualizar** en Power BI. Los nuevos eventos aparecerán entonces junto con el histórico.
