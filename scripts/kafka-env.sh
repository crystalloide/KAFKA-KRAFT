# =============================================================================
#  kafka-env.sh - Variables pratiques pour les TP (à "sourcer")
#  Usage : source ~/KAFKA-KRAFT/scripts/kafka-env.sh
# =============================================================================
export KAFKA_HOME=/opt/kafka
export PATH="$KAFKA_HOME/bin:$PATH"
# Cluster docker compose (3 brokers exposés sur l'hôte)
export BOOTSTRAP="localhost:9092,localhost:9094,localhost:9096"
# Cluster de secours (TP MirrorMaker 2)
export BOOTSTRAP_DR="localhost:9292"
# Noeud "standalone" installé au TP01 (hors Docker)
export BOOTSTRAP_STANDALONE="localhost:9192"
# Racine du projet
export KAFKA_KRAFT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Les outils CLI n'ont pas besoin de plus de 512 Mo de heap
export KAFKA_HEAP_OPTS="-Xmx512m"
alias kt='kafka-topics.sh --bootstrap-server $BOOTSTRAP'
alias kcg='kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP'
alias kcfg='kafka-configs.sh --bootstrap-server $BOOTSTRAP'
echo "Kafka CLI : $(command -v kafka-topics.sh 2>/dev/null || echo 'non installé (voir TP01)') | BOOTSTRAP=$BOOTSTRAP"
