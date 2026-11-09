#!/usr/bin/env bash
# =============================================================================
#  cluster-status.sh - Etat synthétique du cluster KRaft (conteneurs, quorum,
#  brokers, partitions à problème)
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/kafka-env.sh >/dev/null
echo "=== Conteneurs ==="
docker compose ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'
echo; echo "=== Quorum KRaft ==="
kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status 2>/dev/null | grep -E "ClusterId|LeaderId|LeaderEpoch|HighWatermark|MaxFollowerLag|CurrentVoters"
echo; echo "=== Brokers enregistrés ==="
kafka-cluster.sh list-endpoints --bootstrap-server "$BOOTSTRAP" 2>/dev/null
echo; echo "=== Partitions sous-répliquées (vide = OK) ==="
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>/dev/null
echo "=== Partitions sous min.insync.replicas (vide = OK) ==="
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-min-isr-partitions 2>/dev/null
echo "=== Partitions sans leader (vide = OK) ==="
kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --unavailable-partitions 2>/dev/null
