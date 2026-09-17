# Diccionario de datos

Jerarquía canónica:

```text
FCT → LIN/WKA → TU → PS → STEP → RES
```

Silver promueve deliberadamente **24 de los 75** campos fuente de Bronze. Bronze permanece inmutable y completo.

Decisiones principales:

- `FCT_*`: fábrica/sitio.
- `LIN_*`: línea.
- `WKA_*`: área de trabajo; LIN y WKA se conservan separadamente para mantener fidelidad al origen.
- `TU_ID` / `TU_Name`: unidad de apriete o contexto de estación.
- `PS_*`: Process Step / operación; `PS_Comment` es el nombre legible.
- `STEP_*`: subpaso/configuración y especificación de torque.
- ID, número y serial de herramienta: conservados para análisis futuros de equipamiento.
- De `RES_*` solo se promueven `RES_ID`, `RES_Report`, `RES_DateTime` y `RES_FinalTorque`.
- `RES_Report → source_result_status` se conserva como metadata de origen; la conformidad del torque se calcula de forma independiente.
- `RES_VIN` queda excluido y no se utiliza para inferir trazabilidad de producto.
