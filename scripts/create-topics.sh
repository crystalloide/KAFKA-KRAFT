#!/usr/bin/env bash
# =============================================================================
#  create-topics.sh - Création déclarative et idempotente des topics des TP
#  à partir de config/topics/topics.csv (nom;partitions;réplication;configs)
#  Usage : ./scripts/create-topics.sh [fichier.csv]
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/kafka-env.sh >/dev/null
FILE=${1:-config/topics/topics.csv}
grep -vE '^\s*(#|$)' "$FILE" | while IFS=';' read -r name partitions rf configs; do
  args=(--bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$name" --partitions "$partitions" --replication-factor "$rf")
  if [ -n "${configs:-}" ]; then
    IFS=',' read -ra kvs <<< "$configs"
    for kv in "${kvs[@]}"; do args+=(--config "$kv"); done
  fi
  kafka-topics.sh "${args[@]}" >/dev/null
  echo "[OK] ${name} (partitions=${partitions}, RF=${rf}${configs:+, ${configs}})"
done
