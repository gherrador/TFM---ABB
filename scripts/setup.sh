#!/usr/bin/env bash

set -euo pipefail

echo "============================================"
echo " TFM ABB - Setup reproducible"
echo "============================================"

echo
echo "[1/5] Levantando infraestructura..."
docker compose up -d

echo
echo "[2/5] Esperando PostgreSQL..."
until docker exec tfm-postgres sh -lc \
  'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' >/dev/null 2>&1; do
  echo "  PostgreSQL todavía no está listo..."
  sleep 3
done

echo
echo "[3/5] Esperando Kafka..."
until docker exec tfm-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list >/dev/null 2>&1; do
  echo "  Kafka todavía no está listo..."
  sleep 3
done

echo
echo "[4/5] Ejecutando pipeline batch histórico..."
docker compose --profile batch run --rm pipeline

echo
echo "[5/5] Verificando baseline unificado..."
docker exec tfm-postgres sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT SUM(event_count) AS total_eventos FROM analytics.vw_process_daily_kpis;"'

echo
echo "============================================"
echo " Setup completado."
echo "============================================"

echo
echo "Siguiente paso:"
echo "  ./scripts/demo_streaming.sh"