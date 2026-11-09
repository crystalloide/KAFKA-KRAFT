# KAFKA-KRAFT – Travaux pratiques Apache Kafka 4.4 (mode KRaft)

Environnement et handlabs de la formation **Apache Kafka – 2 jours (14 h)** :
Kafka **4.4.0** en mode **KRaft**, Java **21**, Spring Boot **4.1** / Spring for Apache Kafka **4.1**,
**Docker Compose** sous **Ubuntu sur WSL2**.

## Les TP

| Chapitre du programme | Handlab | Durée |
|---|---|---|
| (pré-requis) | [`labs/00-Preparation-environnement-WSL2.md`](labs/00-Preparation-environnement-WSL2.md) | 45 min |
| 1. Introduction et installation | [`labs/01-Introduction-et-installation-Kafka.md`](labs/01-Introduction-et-installation-Kafka.md) | 1 h 30 |
| 2. Installer et configurer un cluster | [`labs/02-Cluster-KRaft.md`](labs/02-Cluster-KRaft.md) | 2 h |
| 3. API producer / consumer (Java, Spring) | [`labs/03-API-Producer-Consumer-Java-Spring.md`](labs/03-API-Producer-Consumer-Java-Spring.md) | 3 h |
| 4. Administrer un cluster | [`labs/04-Administration-cluster.md`](labs/04-Administration-cluster.md) | 1 h 45 |
| 5. Superviser et optimiser | [`labs/05-Supervision-et-optimisation.md`](labs/05-Supervision-et-optimisation.md) | 1 h 30 |
| 6. Sécuriser (synthèse, sans TP) | [`labs/06-Securite-synthese.md`](labs/06-Securite-synthese.md) | 30 min |
| 7. Écosystème : Kafka Connect | [`labs/07a-Kafka-Connect.md`](labs/07a-Kafka-Connect.md) | 1 h 30 |
| 7. Écosystème : Kafka Streams | [`labs/07b-Kafka-Streams.md`](labs/07b-Kafka-Streams.md) | 1 h 30 |
| 7. Écosystème : Schema Registry | [`labs/07c-Schema-Registry.md`](labs/07c-Schema-Registry.md) | 45 min |
| 7. Écosystème : ksqlDB | [`labs/07d-ksqlDB.md`](labs/07d-ksqlDB.md) | 45 min |
| 7. Écosystème : MirrorMaker 2 | [`labs/07e-MirrorMaker2.md`](labs/07e-MirrorMaker2.md) | 45 min |
| 8. Conclusion, ressources, quiz | [`labs/08-Conclusion.md`](labs/08-Conclusion.md) | 30 min |

Le planning détaillé des deux jours figure dans le TP08 (partie G).

## Démarrage rapide

```bash
# Ubuntu sous WSL2, Docker Engine >= 25, Java 21, Maven (voir TP00)
rm -Rf  ~/KAFKA-KRAFT
cd ~ && git clone https://github.com/crystalloide/KAFKA-KRAFT.git && cd KAFKA-KRAFT
./scripts/setup.sh                     # pré-requis + agent JMX Prometheus
docker compose up -d                   # 3 contrôleurs + 3 brokers + Kafka UI
./scripts/create-topics.sh             # topics de la formation
source scripts/kafka-env.sh            # $BOOTSTRAP, PATH vers /opt/kafka/bin (TP01)
./scripts/cluster-status.sh
```

Chaque handlab contient aussi les blocs `cat > fichier <<'EOF_KAFKA_KRAFT'` qui recréent
**à l'identique** les fichiers du dépôt : on peut donc tout reconstruire sans Git.

## Architecture

```text
                      réseau Docker "kafka-net"
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │  controller-1/2/3 (ids 1-3, quorum KRaft :9093)                                 │
 │  broker-1/2/3     (ids 11-13, racks a/b/c)  INTERNAL :19092                     │
 │  broker-4         (id 14, profil "scale")                                       │
 │  kafka-ui :8080   prometheus :9090  grafana :3000  kafka-exporter :9308         │
 │  connect :8083 + postgres   schema-registry :8081   ksqldb-server :8088         │
 │  kafka-dr :9292 + mirror-maker (profil "mirror")    akhq :8090 (profil "consoles")│
 └───────────────────────────────────────────────────────────────────────────────┘
   accès depuis WSL : localhost:9092 (broker-1), :9094 (broker-2), :9096 (broker-3), :9098 (broker-4)
```

| Profil Compose | Services | TP |
|---|---|---|
| *(défaut)* | controller-1..3, broker-1..3, kafka-ui | TP02 → TP08 |
| `scale` | broker-4 | TP02, TP04 |
| `monitoring` | prometheus, grafana, kafka-exporter | TP05 |
| `connect` | connect, postgres | TP07a |
| `schema` | schema-registry | TP07c |
| `ksqldb` | schema-registry, ksqldb-server | TP07d |
| `mirror` | kafka-dr, mirror-maker | TP07e |
| `consoles` | akhq | TP04 |

## Réglages de production appliqués

- KRaft en mode **isolé** (contrôleurs et brokers séparés), `controller.quorum.voters` statique ;
- `default.replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`,
  `auto.create.topics.enable=false`, `broker.rack` (répartition des répliques) ;
- topics internes (`__consumer_offsets`, transactions, share groups) en RF=3 / ISR min 2 ;
- descripteurs de fichiers à 100 000, arrêt propre (`stop_grace_period`), rotation des logs Docker ;
- noyau (`system/99-kafka.conf`) : `vm.swappiness=1`, `vm.max_map_count=262144`, tampons TCP, `dirty_ratio` ;
- tas JVM réduit pour un poste de formation (768 Mo par broker, 6 Go recommandés en production).

## Versions

| Composant | Version |
|---|---|
| Apache Kafka (image `apache/kafka`) | 4.4.0 |
| Kafbat UI | 1.5.0 |
| AKHQ | 0.28.0 |
| Prometheus / Grafana | 3.16.0 / 13.3.0 |
| JMX Exporter / kafka-exporter | 1.7.0 / 1.11.0 |
| Confluent Schema Registry / ksqlDB | 8.4.0 |
| Debezium (PostgreSQL, JDBC) / PostgreSQL | 3.7.1.Final / 18 |
| Java / Maven | 21 / 3.8+ (paquet Ubuntu 24.04) |
| Spring Boot / Spring for Apache Kafka | 4.1.2 / 4.1.2 |

Arrêt : `./scripts/stop-all.sh` (données conservées) ou `./scripts/stop-all.sh --purge` (remise à zéro).
