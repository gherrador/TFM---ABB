# Configuración de dependencias de Control Center Next Gen

Esta carpeta se monta en `/mnt/config` para Prometheus, Alertmanager y Control Center Next Gen.

Archivos de arranque incluidos en el entorno local:

- `prometheus-generated.yml`
- `web-config-prom.yml`
- `alertmanager-generated.yml`
- `web-config-am.yml`
- `trigger_rules-generated.yml`

## Motivo de los archivos `web-config-*.yml`

Las imágenes Confluent de Prometheus y Alertmanager inician sus binarios con `--web.config.file` apuntando a `/mnt/config`.

En este despliegue local no se requiere TLS ni autenticación HTTP básica, por lo que ambos archivos de configuración web contienen un mapping YAML vacío (`{}`), válido como configuración sin seguridad adicional.
