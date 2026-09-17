#!/usr/bin/env bash
set -euo pipefail

KAFKA_PROPERTIES="/etc/kafka/kafka.properties"
TELEMETRY_PROPERTIES="/opt/tfm-kafka/telemetry.properties"

if [[ ! -f "${KAFKA_PROPERTIES}" ]]; then
  echo "ERROR: ${KAFKA_PROPERTIES} does not exist after Confluent configure step." >&2
  exit 1
fi

if [[ ! -f "${TELEMETRY_PROPERTIES}" ]]; then
  echo "ERROR: ${TELEMETRY_PROPERTIES} is not mounted." >&2
  exit 1
fi

# Elimina variantes malformadas antiguas procedentes de intentos previos basados en variables de entorno.
sed -i   -e '/^confluent\.telemetry\.exporter[_-]c3\./d'   -e '/^confluent\.telemetry\.exporter\._c3\./d'   -e '/^confluent\.telemetry\.remoteconfig[_-]confluent\./d'   -e '/^confluent\.telemetry\.remoteconfig\._confluent\./d'   "${KAFKA_PROPERTIES}"

printf '\n# TFM Control Center Next Gen telemetry override\n' >> "${KAFKA_PROPERTIES}"
cat "${TELEMETRY_PROPERTIES}" >> "${KAFKA_PROPERTIES}"

echo "Injected exact Control Center telemetry properties:"
grep -E '^confluent\.telemetry\.(exporter\._c3|remoteconfig\._confluent)\.' "${KAFKA_PROPERTIES}"   | sed -E 's/(api\.key|api\.secret)=.*/\1=[hidden]/'
