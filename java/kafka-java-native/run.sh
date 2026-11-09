#!/usr/bin/env bash
# =============================================================================
#  run.sh - Lance une classe d'exemple du projet kafka-java-native
#  Usage : ./run.sh <package.Classe> [arguments...]
#  Ex.   : ./run.sh producer.SimpleProducer test-formation 10
#          ./run.sh consumer.SimpleConsumer orders groupe-java
#  Variable optionnelle : KAFKA_BOOTSTRAP_SERVERS (défaut : localhost:9092,localhost:9094,localhost:9096)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"
if [ $# -lt 1 ]; then
  echo "Usage : $0 <package.Classe> [arguments...]"
  echo "Classes disponibles :"
  find src/main/java -name '*.java' -exec grep -l 'static void main' {} \; \
    | sed 's|src/main/java/fr/formation/kafka/||; s|\.java$||; s|/|.|g' | sort | sed 's/^/  /'
  exit 1
fi
CLASS="fr.formation.kafka.$1"; shift
exec mvn -q compile exec:java -Dexec.mainClass="$CLASS" -Dexec.args="$*"
