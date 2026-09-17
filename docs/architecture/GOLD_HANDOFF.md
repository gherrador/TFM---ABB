# Traspaso Silver → Gold

Silver proporciona la semántica a nivel de evento y conserva la jerarquía:

```text
FCT → LIN/WKA → TU → PS → STEP → resultado
```

Gold agrega únicamente eventos Silver válidos por DQ sin reinterpretar la jerarquía fuente. La dimensión de proceso es sensible a la configuración y la tabla de hechos diaria reconcilia exactamente con los conteos de eventos Silver considerados.

Gold requiere timestamp y trazabilidad del evento, contexto del proceso, descriptores de herramienta, especificación de torque, torque aplicado y los campos derivados `torque_applicable` y `torque_spec_status`.
