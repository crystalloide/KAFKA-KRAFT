#!/usr/bin/env bash
# =============================================================================
#  download-connect-plugins.sh - Télécharge les plugins Debezium (Maven Central)
#   - debezium-connector-postgres : connecteur SOURCE CDC PostgreSQL
#   - debezium-connector-jdbc     : connecteur SINK JDBC (pilote PostgreSQL inclus)
#  Les plugins sont extraits dans connect/plugins/ (monté dans /opt/connect-plugins)
#  Usage : ./scripts/download-connect-plugins.sh
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
DBZ_VERSION=$(grep -E '^DEBEZIUM_VERSION=' .env | cut -d= -f2 | tr -d '"')
BASE=https://repo1.maven.org/maven2/io/debezium
mkdir -p connect/plugins /tmp/kafka-kraft-plugins

for plugin in debezium-connector-postgres debezium-connector-jdbc; do
  if [ -d "connect/plugins/${plugin}" ] && ls connect/plugins/${plugin}/*.jar >/dev/null 2>&1; then
    echo "[OK] ${plugin} déjà présent"
    continue
  fi
  archive="/tmp/kafka-kraft-plugins/${plugin}-${DBZ_VERSION}-plugin.tar.gz"
  url="${BASE}/${plugin}/${DBZ_VERSION}/${plugin}-${DBZ_VERSION}-plugin.tar.gz"
  echo "-> ${url}"
  curl -fSL --retry 3 --connect-timeout 20 -o "${archive}" "${url}"
  tar -xzf "${archive}" -C connect/plugins
  echo "[OK] ${plugin} ${DBZ_VERSION} extrait dans connect/plugins/"
done

find connect/plugins -type d -exec chmod 755 {} \;
find connect/plugins -type f -exec chmod 644 {} \;
echo
ls -1 connect/plugins
echo
echo "Redémarrez le worker pour qu'il détecte les plugins : docker compose --profile connect restart connect"
