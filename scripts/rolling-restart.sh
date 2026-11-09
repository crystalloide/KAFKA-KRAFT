#!/usr/bin/env bash
# =============================================================================
#  rolling-restart.sh - Redémarrage "à chaud" (rolling restart) des brokers :
#  un broker à la fois, en attendant le retour à 0 partition sous-répliquée.
#  Procédure identique pour une montée de version (changer KAFKA_IMAGE dans .env
#  puis utiliser "docker compose up -d <broker>" au lieu de "restart").
#  Usage : ./scripts/rolling-restart.sh [restart|recreate]
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/kafka-env.sh >/dev/null
MODE=${1:-restart}
SCOPE=${2:-brokers}

wait_healthy() {
  local c=$1
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' "$c" 2>/dev/null)" = "healthy" ] && return 0
    sleep 5
  done
  echo "Timeout : $c n'est pas redevenu healthy"; exit 1
}

wait_no_urp() {
  for _ in $(seq 1 60); do
    if [ -z "$(kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null)" ]; then
      return 0
    fi
    sleep 5
  done
  echo "Timeout : des partitions sont toujours sous-répliquées"; exit 1
}

restart_node() {
  if [ "$MODE" = "recreate" ]; then
    docker compose up -d --no-deps --force-recreate "$1"
  else
    docker compose restart "$1"
  fi
}

if [ "$SCOPE" = "all" ]; then
  for controller in controller-1 controller-2 controller-3; do
    echo "=== $(date +%T) ${controller} : ${MODE} ==="
    restart_node "$controller"
    wait_healthy "$controller"
    sleep 10   # le contrôleur rattrape le journal des métadonnées
    kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status | grep -E "LeaderId|MaxFollowerLag:"
  done
fi

for broker in broker-1 broker-2 broker-3; do
  echo "=== $(date +%T) ${broker} : ${MODE} ==="
  restart_node "$broker"
  wait_healthy "$broker"
  echo "    ${broker} healthy, attente de la resynchronisation des réplicas..."
  wait_no_urp
  echo "    0 partition sous-répliquée : noeud suivant"
done
echo "=== Rolling restart terminé ==="
