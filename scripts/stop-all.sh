#!/usr/bin/env bash
# =============================================================================
#  stop-all.sh - Arrête TOUS les services (tous profils confondus)
#  Usage : ./scripts/stop-all.sh          -> arrêt (données conservées)
#          ./scripts/stop-all.sh --purge  -> suppression des conteneurs ET des volumes
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROFILES=scale,monitoring,connect,schema,ksqldb,mirror,consoles
if [ "${1:-}" = "--purge" ]; then
  docker compose down -v --remove-orphans
  echo "Conteneurs et volumes supprimés (cluster remis à zéro)."
else
  docker compose stop
  echo "Services arrêtés (données conservées). Relance : docker compose up -d"
fi
