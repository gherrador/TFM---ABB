# Auditoría final de Silver

Silver constituye la capa canónica a nivel de evento. Bronze permanece inmutable con las 75 columnas fuente, mientras que Silver proyecta únicamente los 24 campos de negocio necesarios para el TFM y para posibles análisis futuros de equipamiento.

La jerarquía utilizada es:

```text
fábrica → línea/área de trabajo → unidad de apriete → paso de proceso → subpaso → resultado
```

`PS_Comment` representa el nombre de la operación/Process Step y `STEP_Number` representa un subpaso dentro de dicho PS. Los campos de controlador y diagnósticos fuente no utilizados quedan excluidos de Silver.

Solo se retienen cuatro campos `RES_*`: `RES_ID`, `RES_Report`, `RES_DateTime` y `RES_FinalTorque`. `RES_Report` se transforma en `source_result_status` y se conserva únicamente como referencia del origen y apoyo DQ. La conformidad del torque se deriva independientemente del torque final y de los límites mínimo/máximo configurados.

`RES_VIN` no se promueve a Silver y no se infiere ningún KPI de trazabilidad de producto a partir de ese campo. La trazabilidad técnica se mantiene mediante `source_file + source_row_number`, mientras que `result_id` conserva `RES_ID`.

La identidad de proceso utilizada en Gold combina TU, PS, nombre del PS, STEP y tipo de STEP para evitar colisiones derivadas de identificadores reutilizados con semánticas distintas.
