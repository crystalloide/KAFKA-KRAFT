#!/usr/bin/env bash
# =============================================================================
#  setup.sh - Prépare l'environnement KAFKA-KRAFT avant "docker compose up"
#   - vérifie les pré-requis (Docker, Compose, réglages système)
#   - télécharge l'agent Prometheus JMX Exporter (obligatoire : monté dans
#     tous les conteneurs Kafka via KAFKA_OPTS=-javaagent:...)
#   - crée les répertoires de données de Kafka Connect
#  Usage : bash scripts/setup.sh   (fonctionne même si le bit "exécutable"
#          des scripts a été perdu, puis le rétablit)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

get_env() { grep -E "^$1=" .env | head -1 | cut -d= -f2- | tr -d '"'; }

ok()   { printf '  \033[32m[OK]\033[0m   %s\n' "$*"; }
warn() { printf '  \033[33m[WARN]\033[0m %s\n' "$*"; }
ko()   { printf '  \033[31m[KO]\033[0m   %s\n' "$*"; }

download() { # download <destination> <url1> [url2...]
  local dest="$1"; shift
  for url in "$@"; do
    echo "  -> téléchargement : $url"
    if curl -fSL --retry 3 --connect-timeout 20 -o "${dest}.part" "$url"; then
      mv "${dest}.part" "$dest"; return 0
    fi
  done
  rm -f "${dest}.part"; return 1
}

echo "=== 0. Fichiers du projet ==="
if [ ! -f .env ] && [ -f env.txt ]; then
  cp env.txt .env && warn ".env absent : recréé à partir de env.txt"
fi
for f in .env docker-compose.yml config/jmx-exporter/kafka.yml; do
  [ -f "$f" ] && ok "$f présent" || { ko "$f absent : récupérez le projet complet (TP00 §4)"; exit 1; }
done

echo "=== 1. Pré-requis ==="
command -v docker >/dev/null && ok "docker : $(docker --version)" || { ko "docker absent (voir TP00)"; exit 1; }
docker compose version >/dev/null 2>&1 && ok "docker compose : $(docker compose version --short)" || { ko "plugin docker compose absent"; exit 1; }
docker info >/dev/null 2>&1 && ok "démon Docker joignable" || { ko "démon Docker injoignable (sudo systemctl start docker ?)"; exit 1; }
for c in curl jq; do command -v "$c" >/dev/null && ok "$c présent" || warn "$c absent : sudo apt-get install -y $c"; done

MAPS=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
[ "$MAPS" -ge 262144 ] && ok "vm.max_map_count = $MAPS" || warn "vm.max_map_count = $MAPS (recommandé >= 262144, voir TP00 §5)"
SWAP=$(sysctl -n vm.swappiness 2>/dev/null || echo 60)
[ "$SWAP" -le 10 ] && ok "vm.swappiness = $SWAP" || warn "vm.swappiness = $SWAP (recommandé : 1, voir TP00 §5)"
MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
[ "$MEM_GB" -ge 9 ] && ok "mémoire WSL : ${MEM_GB} Go" || warn "mémoire WSL : ${MEM_GB} Go (recommandé >= 10 Go, voir .wslconfig)"

echo "=== 2. Agent Prometheus JMX Exporter ==="
JMX_VERSION=$(get_env JMX_EXPORTER_VERSION)
JMX_JAR=config/jmx-exporter/jmx_prometheus_javaagent.jar
if [ -s "$JMX_JAR" ] && [ "$(head -c 2 "$JMX_JAR")" = "PK" ]; then
  ok "agent déjà présent : $JMX_JAR"
else
  rm -rf "$JMX_JAR"   # supprime un éventuel répertoire créé par erreur par Docker
  download "$JMX_JAR" \
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar" \
    "https://github.com/prometheus/jmx_exporter/releases/download/${JMX_VERSION}/jmx_prometheus_javaagent-${JMX_VERSION}.jar" \
    || { ko "échec du téléchargement de l'agent JMX Exporter"; exit 1; }
  [ "$(head -c 2 "$JMX_JAR")" = "PK" ] && ok "agent JMX Exporter ${JMX_VERSION} téléchargé" || { ko "fichier invalide : $JMX_JAR"; exit 1; }
fi
chmod 644 "$JMX_JAR" config/jmx-exporter/*.yml

echo "=== 3. Répertoires Kafka Connect ==="
mkdir -p connect/data/input connect/data/output connect/plugins
chmod 777 connect/data connect/data/input connect/data/output   # écriture par l'utilisateur "appuser" du conteneur
if [ ! -f connect/data/input/films.txt ] && [ -f data/films.txt ]; then
  cp data/films.txt connect/data/input/films.txt
fi
[ -f connect/data/input/films.txt ] && chmod 666 connect/data/input/films.txt
ok "connect/data/{input,output} prêts"

echo "=== 4. Droits de lecture des fichiers montés dans les conteneurs ==="
for d in config data; do
  [ -d "$d" ] || continue
  find "$d" -type d -exec chmod 755 {} \;
  find "$d" -type f -exec chmod 644 {} \;
done
chmod +x scripts/*.sh
[ -f java/kafka-java-native/run.sh ] && chmod +x java/kafka-java-native/run.sh
ok "droits positionnés (scripts exécutables)"

echo
echo "Environnement prêt. Démarrage du cluster :  docker compose up -d"
