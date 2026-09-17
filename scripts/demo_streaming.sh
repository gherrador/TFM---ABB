#!/usr/bin/env bash

set -euo pipefail

COUNT="${1:-100}"
INTERVAL="${2:-0.1}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-60}"
WAIT_SECONDS="${WAIT_SECONDS:-2}"

get_total() {
  docker exec tfm-postgres sh -lc \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "SELECT COALESCE(SUM(event_count), 0) FROM analytics.vw_process_daily_kpis;"' \
    | tr -d '[:space:]'
}

validate_total() {
  local value="$1"
  local description="$2"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "ERROR: PostgreSQL devolvió un valor no válido para ${description}: '${value}'" >&2
    exit 1
  fi
}

if [[ ! "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -le 0 ]; then
  echo "ERROR: COUNT debe ser un número entero positivo." >&2
  exit 1
fi

if [[ ! "$INTERVAL" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "ERROR: INTERVAL debe ser un número mayor o igual que cero." >&2
  exit 1
fi

echo "============================================"
echo " TFM ABB - Demo Streaming"
echo "============================================"

BASELINE="$(get_total)"
validate_total "$BASELINE" "el baseline"

EXPECTED=$((10#$BASELINE + 10#$COUNT))

echo
echo "[1/4] Total antes del streaming: $BASELINE"
echo "Eventos a generar: $COUNT"
echo "Total esperado: $EXPECTED"

echo
echo "[2/4] Generando eventos..."
docker compose --profile producer run --rm producer \
  --count "$COUNT" \
  --interval "$INTERVAL"

echo
echo "[3/4] Esperando a Spark/PostgreSQL..."

CURRENT="$BASELINE"

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
  CURRENT="$(get_total)"
  validate_total "$CURRENT" "el total actual"

  echo "  Total actual: $CURRENT / esperado: $EXPECTED"

  if [ "$CURRENT" -ge "$EXPECTED" ]; then
    break
  fi

  sleep "$WAIT_SECONDS"
done

if [ "$CURRENT" -lt "$EXPECTED" ]; then
  echo "ERROR: timeout esperando el procesamiento streaming." >&2
  echo "Total alcanzado: $CURRENT / esperado: $EXPECTED" >&2
  exit 1
fi

echo
echo "[4/4] Validación final..."

FINAL="$(get_total)"
validate_total "$FINAL" "el total final"

if [ "$FINAL" -eq "$EXPECTED" ]; then
  echo "PASS: $BASELINE + $COUNT = $FINAL"
else
  echo "ERROR: se esperaban $EXPECTED eventos, pero PostgreSQL contiene $FINAL." >&2
  exit 1
fi

echo
echo "Demo streaming completada correctamente."
echo "Ahora abrí Power BI y ejecutá: Inicio -> Actualizar"