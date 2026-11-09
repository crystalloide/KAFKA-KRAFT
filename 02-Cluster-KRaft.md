# TP02 – Installer et configurer un cluster Kafka KRaft

> **Chapitre 2 du programme** – Objectif pédagogique n°2
> Durée : 2 h
> Pré-requis : TP00 et TP01 (outils CLI Kafka 4.4.0 dans `/opt/kafka/bin`)
> Reprend et met à jour : TP02, TP03, TP03a, TP03b, TP29*, TP32 (ZooKeeper et images obsolètes retirés)

---

## Objectifs

- Mettre en place un cluster **KRaft "production-like"** : **3 contrôleurs dédiés + 3 brokers**
- Comprendre la configuration réseau (**listeners**) et les réglages de production
- Administrer les topics : création, description, configuration, suppression
- Observer le **partitionnement** et la **réplication** (leader, ISR, `min.insync.replicas`, `acks`)
- Tester la **tolérance aux pannes** (brokers et contrôleurs) et la **scalabilité** (ajout de partitions,
  ajout d'un broker, réaffectation des partitions)

---

## Partie A – Rappels : clustering KRaft

```text
                      Quorum KRaft (Raft) - métadonnées du cluster (__cluster_metadata)
             ┌───────────────────┐   ┌───────────────────┐   ┌───────────────────┐
             │ controller-1 (1)  │   │ controller-2 (2)  │   │ controller-3 (3)  │
             │  leader (actif)   │◄─►│  follower (voter) │◄─►│  follower (voter) │
             └─────────▲─────────┘   └───────────────────┘   └───────────────────┘
                       │ enregistrement, heartbeats, réplication des métadonnées (port 9093)
     ┌─────────────────┼─────────────────────────┬──────────────────────────┐
┌────┴─────────┐ ┌─────┴────────┐ ┌──────────────┴┐                  (broker-4, node 14,
│ broker-1 (11)│ │ broker-2 (12)│ │ broker-3 (13) │                   profil "scale")
│ rack-a       │ │ rack-b       │ │ rack-c        │
│ INTERNAL:19092 │ INTERNAL:19092 │ INTERNAL:19092│  <- réseau Docker (réplication, Connect, UI...)
│ EXTERNAL:9092  │ EXTERNAL:9094  │ EXTERNAL:9096 │  <- hôte WSL / Windows (CLI, Java, IntelliJ)
└──────────────┘ └──────────────┘ └───────────────┘
```

| Notion | Description |
|---|---|
| `process.roles` | `controller`, `broker` ou les deux (mode combiné, réservé au développement) |
| `node.id` | Identifiant unique dans le cluster (contrôleurs **et** brokers) |
| `controller.quorum.voters` | Quorum **statique** : `id@hôte:port` de chaque contrôleur |
| `controller.quorum.bootstrap.servers` | Quorum **dynamique** (KIP-853, Kafka ≥ 3.9) : ajout/retrait de contrôleurs à chaud |
| `listeners` | Interfaces d'écoute (`NOM://hôte:port`) |
| `advertised.listeners` | Adresses **communiquées aux clients** : doivent être joignables par eux ! |
| `listener.security.protocol.map` | Protocole de chaque listener (PLAINTEXT, SSL, SASL_SSL...) |
| `inter.broker.listener.name` | Listener utilisé pour la réplication entre brokers |
| `controller.listener.names` | Listener(s) du quorum de contrôleurs |

> En KRaft, ce sont les **contrôleurs** qui appliquent `num.partitions` et `default.replication.factor`
> lors d'une création de topic : ces valeurs sont donc positionnées sur les contrôleurs **et** les brokers.

**Réglages de production appliqués dans `docker-compose.yml` :**

| Paramètre | Valeur | Rôle |
|---|---|---|
| `default.replication.factor` | 3 | Chaque partition existe sur 3 brokers |
| `min.insync.replicas` | 2 | Une écriture `acks=all` exige 2 réplicas à jour : tolère 1 panne sans perte |
| `offsets.topic.replication.factor`, `transaction.state.log.replication.factor` | 3 | Topics internes répliqués |
| `transaction.state.log.min.isr` | 2 | Idem pour les transactions |
| `auto.create.topics.enable` | false | Pas de création implicite de topics |
| `unclean.leader.election.enable` | false | Jamais d'élection d'un réplica en retard (pas de perte de données) |
| `broker.rack` | rack-a/b/c | Répartition des réplicas sur des racks (zones) différents |
| `group.initial.rebalance.delay.ms` | 3000 | Evite les rééquilibrages en rafale au démarrage des consumers |
| `ulimits nofile` | 100 000 | Descripteurs de fichiers |
| `stop_grace_period` | 60 s | Laisse le temps à l'**arrêt contrôlé** (transfert des leaders) |
| Heap JVM | 768 Mo (lab) | **6 Go** typiquement en production (le reste de la RAM = page cache) |

---

## Partie B – Mise en place du cluster avec Docker Compose

### B.1 Fichiers du projet

```bash
cd ~/KAFKA-KRAFT
```

Variables d'environnement (versions des images, identifiant du cluster, mémoire JVM) :

```bash
cd ~/KAFKA-KRAFT
cat > .env <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  KAFKA-KRAFT - Variables d'environnement de l'environnement docker compose
#  (versions vérifiées en novembre 2026)
# =============================================================================
COMPOSE_PROJECT_NAME=kafka-kraft

# --- Identifiant unique du cluster KRaft (22 caractères, base64 URL-safe) ----
#     Regénérable avec : /opt/kafka/bin/kafka-storage.sh random-uuid
#     ATTENTION : ne le changez que sur un cluster vierge (docker compose down -v)
CLUSTER_ID=PIYzDzl8TomQ84Cnrxi0gA
# Identifiant du cluster de secours (TP MirrorMaker 2)
DR_CLUSTER_ID=Pkdd4qI5SaGzACER4PM-6A

# --- Images ------------------------------------------------------------------
KAFKA_IMAGE=apache/kafka:4.4.0
KAFKA_UI_IMAGE=ghcr.io/kafbat/kafka-ui:v1.5.0
AKHQ_IMAGE=tchiotludo/akhq:0.28.0
PROMETHEUS_IMAGE=prom/prometheus:v3.16.0
GRAFANA_IMAGE=grafana/grafana:13.3.0
KAFKA_EXPORTER_IMAGE=danielqsj/kafka-exporter:v1.11.0
SCHEMA_REGISTRY_IMAGE=confluentinc/cp-schema-registry:8.4.0
KSQLDB_IMAGE=confluentinc/cp-ksqldb-server:8.4.0
POSTGRES_IMAGE=postgres:18

# --- Artefacts téléchargés par scripts/setup.sh -------------------------------
KAFKA_VERSION=4.4.0
JMX_EXPORTER_VERSION=1.7.0
DEBEZIUM_VERSION=3.7.1.Final

# --- Mémoire JVM (valeurs "laboratoire" : en production, brokers 6 Go) --------
CONTROLLER_HEAP_OPTS="-Xms384m -Xmx384m"
BROKER_HEAP_OPTS="-Xms768m -Xmx768m"
CONNECT_HEAP_OPTS="-Xms512m -Xmx1g"
EOF_KAFKA_KRAFT
```

Définition du cluster (3 contrôleurs, 3 brokers, Kafka UI et composants optionnels par profils) :

```bash
cd ~/KAFKA-KRAFT
cat > docker-compose.yml <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  KAFKA-KRAFT - Environnement de formation Apache Kafka 4.4.0 (mode KRaft)
#
#  Architecture "production-like" (mode isolé, recommandé en production) :
#    - 3 contrôleurs KRaft dédiés (quorum Raft des métadonnées) : nodes 1, 2, 3
#    - 3 brokers dédiés (données)                               : nodes 11, 12, 13
#    - Kafka UI (console web)                                   : http://localhost:8080
#
#  Composants optionnels activés par profil (--profile <nom>) :
#    scale      : broker-4 (node 14) pour le TP de scalabilité
#    monitoring : Prometheus (9090), Grafana (3000), kafka-exporter (9308)
#    connect    : Kafka Connect distribué (8083) + PostgreSQL (CDC Debezium)
#    schema     : Schema Registry (8081)
#    ksqldb     : ksqlDB (8088) (+ Schema Registry)
#    mirror     : cluster de secours kafka-dr (9292) + MirrorMaker 2
#    consoles   : AKHQ (8090), console web alternative à Kafka UI
#
#  Pré-requis : ./scripts/setup.sh (télécharge l'agent JMX Exporter, etc.)
# =============================================================================
name: kafka-kraft

# -----------------------------------------------------------------------------
#  Blocs réutilisables (ancres YAML)
# -----------------------------------------------------------------------------
x-kafka-base: &kafka-base
  image: ${KAFKA_IMAGE}
  restart: unless-stopped
  networks:
    - kafka-net
  ulimits:                       # descripteurs de fichiers : >= 100 000 en production
    nofile:
      soft: 100000
      hard: 100000
  logging:                       # rotation des logs Docker
    driver: json-file
    options:
      max-size: "20m"
      max-file: "3"

# Valeurs par défaut des topics : en mode KRaft, c'est le CONTROLEUR qui applique
# num.partitions / default.replication.factor lors d'une création de topic.
x-topic-defaults: &topic-defaults
  KAFKA_NUM_PARTITIONS: 3
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2

x-controller-env: &controller-env
  <<: *topic-defaults
  CLUSTER_ID: ${CLUSTER_ID}
  KAFKA_PROCESS_ROLES: "controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "1@controller-1:9093,2@controller-2:9093,3@controller-3:9093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_LISTENERS: "CONTROLLER://:9093"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT"
  KAFKA_LOG_DIRS: "/var/lib/kafka/data"
  KAFKA_HEAP_OPTS: ${CONTROLLER_HEAP_OPTS}
  KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/kafka.yml"

x-broker-env: &broker-env
  <<: *topic-defaults
  CLUSTER_ID: ${CLUSTER_ID}
  KAFKA_PROCESS_ROLES: "broker"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "1@controller-1:9093,2@controller-2:9093,3@controller-3:9093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
  KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
  KAFKA_LOG_DIRS: "/var/lib/kafka/data"
  # --- Paramétrage "production" -------------------------------------------
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE: "false"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 2
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 3000
  KAFKA_NUM_NETWORK_THREADS: 3
  KAFKA_NUM_IO_THREADS: 8
  KAFKA_NUM_REPLICA_FETCHERS: 2
  KAFKA_NUM_RECOVERY_THREADS_PER_DATA_DIR: 2
  KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
  KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
  KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
  KAFKA_LOG_RETENTION_HOURS: 168
  KAFKA_LOG_SEGMENT_BYTES: 1073741824
  KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 300000
  KAFKA_REPLICA_LAG_TIME_MAX_MS: 30000
  KAFKA_HEAP_OPTS: ${BROKER_HEAP_OPTS}
  KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/kafka.yml"

# Contrôle de santé d'un contrôleur : port Raft ouvert
x-controller-healthcheck: &controller-healthcheck
  test: ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9093"]
  interval: 10s
  timeout: 5s
  retries: 10
  start_period: 20s

# Contrôle de santé d'un broker : requête réelle sur le listener interne.
# KAFKA_OPTS est vidé pour ne pas relancer l'agent JMX (port 9404 déjà pris).
x-broker-healthcheck: &broker-healthcheck
  test: ["CMD-SHELL", "KAFKA_OPTS= KAFKA_HEAP_OPTS=-Xmx128m /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:19092 > /dev/null 2>&1"]
  interval: 30s
  timeout: 25s
  retries: 6
  start_period: 90s
  start_interval: 5s

x-jmx-volume: &jmx-volume "./config/jmx-exporter:/opt/jmx-exporter:ro"

x-depends-on-controllers: &depends-on-controllers
  controller-1:
    condition: service_healthy
  controller-2:
    condition: service_healthy
  controller-3:
    condition: service_healthy

x-depends-on-brokers: &depends-on-brokers
  broker-1:
    condition: service_healthy
  broker-2:
    condition: service_healthy
  broker-3:
    condition: service_healthy

services:
  # ===========================================================================
  #  Contrôleurs KRaft (quorum de métadonnées) - non exposés sur l'hôte
  # ===========================================================================
  controller-1:
    <<: *kafka-base
    hostname: controller-1
    container_name: controller-1
    stop_grace_period: 30s
    environment:
      <<: *controller-env
      KAFKA_NODE_ID: 1
    volumes:
      - controller-1-data:/var/lib/kafka/data
      - *jmx-volume
    healthcheck: *controller-healthcheck

  controller-2:
    <<: *kafka-base
    hostname: controller-2
    container_name: controller-2
    stop_grace_period: 30s
    environment:
      <<: *controller-env
      KAFKA_NODE_ID: 2
    volumes:
      - controller-2-data:/var/lib/kafka/data
      - *jmx-volume
    healthcheck: *controller-healthcheck

  controller-3:
    <<: *kafka-base
    hostname: controller-3
    container_name: controller-3
    stop_grace_period: 30s
    environment:
      <<: *controller-env
      KAFKA_NODE_ID: 3
    volumes:
      - controller-3-data:/var/lib/kafka/data
      - *jmx-volume
    healthcheck: *controller-healthcheck

  # ===========================================================================
  #  Brokers - listener INTERNAL (réseau Docker) et EXTERNAL (hôte WSL / Windows)
  # ===========================================================================
  broker-1:
    <<: *kafka-base
    hostname: broker-1
    container_name: broker-1
    stop_grace_period: 60s          # temps laissé à l'arrêt contrôlé (controlled shutdown)
    ports:
      - "9092:9092"
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 11
      KAFKA_BROKER_RACK: "rack-a"
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://broker-1:19092,EXTERNAL://localhost:9092"
    volumes:
      - broker-1-data:/var/lib/kafka/data
      - *jmx-volume
    depends_on: *depends-on-controllers
    healthcheck: *broker-healthcheck

  broker-2:
    <<: *kafka-base
    hostname: broker-2
    container_name: broker-2
    stop_grace_period: 60s
    ports:
      - "9094:9094"
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 12
      KAFKA_BROKER_RACK: "rack-b"
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9094"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://broker-2:19092,EXTERNAL://localhost:9094"
    volumes:
      - broker-2-data:/var/lib/kafka/data
      - *jmx-volume
    depends_on: *depends-on-controllers
    healthcheck: *broker-healthcheck

  broker-3:
    <<: *kafka-base
    hostname: broker-3
    container_name: broker-3
    stop_grace_period: 60s
    ports:
      - "9096:9096"
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 13
      KAFKA_BROKER_RACK: "rack-c"
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9096"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://broker-3:19092,EXTERNAL://localhost:9096"
    volumes:
      - broker-3-data:/var/lib/kafka/data
      - *jmx-volume
    depends_on: *depends-on-controllers
    healthcheck: *broker-healthcheck

  # Broker supplémentaire (TP scalabilité) : docker compose --profile scale up -d
  broker-4:
    <<: *kafka-base
    profiles: ["scale"]
    hostname: broker-4
    container_name: broker-4
    stop_grace_period: 60s
    ports:
      - "9098:9098"
    environment:
      <<: *broker-env
      KAFKA_NODE_ID: 14
      KAFKA_BROKER_RACK: "rack-a"
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9098"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://broker-4:19092,EXTERNAL://localhost:9098"
    volumes:
      - broker-4-data:/var/lib/kafka/data
      - *jmx-volume
    depends_on: *depends-on-controllers
    healthcheck: *broker-healthcheck

  # ===========================================================================
  #  Console web Kafka UI (kafbat) : http://localhost:8080
  # ===========================================================================
  kafka-ui:
    image: ${KAFKA_UI_IMAGE}
    container_name: kafka-ui
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "8080:8080"
    environment:
      DYNAMIC_CONFIG_ENABLED: "true"
      KAFKA_CLUSTERS_0_NAME: "kafka-kraft"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "broker-1:19092,broker-2:19092,broker-3:19092"
      KAFKA_CLUSTERS_0_METRICS_TYPE: "PROMETHEUS"
      KAFKA_CLUSTERS_0_METRICS_PORT: 9404
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: "http://schema-registry:8081"
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: "connect"
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: "http://connect:8083"
      KAFKA_CLUSTERS_0_KSQLDBSERVER: "http://ksqldb-server:8088"
      KAFKA_CLUSTERS_1_NAME: "kafka-dr"
      KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS: "kafka-dr:19092"
    depends_on: *depends-on-brokers

  # ===========================================================================
  #  Profil "monitoring" : Prometheus + Grafana + kafka-exporter
  # ===========================================================================
  prometheus:
    image: ${PROMETHEUS_IMAGE}
    profiles: ["monitoring"]
    container_name: prometheus
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "9090:9090"
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
      - "--storage.tsdb.retention.time=7d"
      - "--web.enable-lifecycle"
    extra_hosts:
      - "host.docker.internal:host-gateway"   # pour scruter l'application Spring Boot lancée sous WSL
    volumes:
      - ./config/prometheus:/etc/prometheus:ro
      - prometheus-data:/prometheus

  grafana:
    image: ${GRAFANA_IMAGE}
    profiles: ["monitoring"]
    container_name: grafana
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: "admin"
      GF_SECURITY_ADMIN_PASSWORD: "kafka"
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: "/etc/grafana/dashboards/kafka-overview.json"
    volumes:
      - ./config/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./config/grafana/dashboards:/etc/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
    depends_on:
      - prometheus

  kafka-exporter:
    image: ${KAFKA_EXPORTER_IMAGE}
    profiles: ["monitoring"]
    container_name: kafka-exporter
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "9308:9308"
    command:
      - "--kafka.server=broker-1:19092"
      - "--kafka.server=broker-2:19092"
      - "--kafka.server=broker-3:19092"
      - "--kafka.version=3.9.0"
      - "--topic.filter=^[^_].*"
      - "--group.filter=.*"
    depends_on: *depends-on-brokers

  # ===========================================================================
  #  Profil "connect" : Kafka Connect distribué + PostgreSQL (source CDC)
  # ===========================================================================
  connect:
    <<: *kafka-base
    profiles: ["connect"]
    hostname: connect
    container_name: connect
    ports:
      - "8083:8083"
    command: ["/opt/kafka/bin/connect-distributed.sh", "/etc/kafka-connect/connect-distributed.properties"]
    environment:
      KAFKA_HEAP_OPTS: ${CONNECT_HEAP_OPTS}
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=9404:/opt/jmx-exporter/kafka-connect.yml"
    volumes:
      - ./config/connect/connect-distributed.properties:/etc/kafka-connect/connect-distributed.properties:ro
      - *jmx-volume
      - ./connect/plugins:/opt/connect-plugins:ro
      - ./connect/data:/data
    healthcheck:
      test: ["CMD-SHELL", "wget -q -O /dev/null http://localhost:8083/connectors"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 60s
    depends_on: *depends-on-brokers

  postgres:
    image: ${POSTGRES_IMAGE}
    profiles: ["connect"]
    hostname: postgres
    container_name: postgres
    restart: unless-stopped
    networks:
      - kafka-net
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
    environment:
      POSTGRES_USER: "postgres"
      POSTGRES_PASSWORD: "postgres"
      POSTGRES_DB: "formation"
    volumes:
      - ./config/postgres/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d formation"]
      interval: 10s
      timeout: 5s
      retries: 10

  # ===========================================================================
  #  Profils "schema" et "ksqldb" : Schema Registry et ksqlDB (Confluent Community)
  # ===========================================================================
  schema-registry:
    image: ${SCHEMA_REGISTRY_IMAGE}
    profiles: ["schema", "ksqldb"]
    hostname: schema-registry
    container_name: schema-registry
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: "schema-registry"
      SCHEMA_REGISTRY_LISTENERS: "http://0.0.0.0:8081"
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "PLAINTEXT://broker-1:19092,PLAINTEXT://broker-2:19092,PLAINTEXT://broker-3:19092"
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: "_schemas"
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR: 3
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: "backward"
      SCHEMA_REGISTRY_HEAP_OPTS: "-Xms256m -Xmx512m"
    volumes:
      - ./data/avro:/data/avro:ro
    depends_on: *depends-on-brokers

  ksqldb-server:
    image: ${KSQLDB_IMAGE}
    profiles: ["ksqldb"]
    hostname: ksqldb-server
    container_name: ksqldb-server
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "8088:8088"
    environment:
      KSQL_CONFIG_DIR: "/etc/ksql"
      KSQL_BOOTSTRAP_SERVERS: "broker-1:19092,broker-2:19092,broker-3:19092"
      KSQL_HOST_NAME: "ksqldb-server"
      KSQL_LISTENERS: "http://0.0.0.0:8088"
      KSQL_CACHE_MAX_BYTES_BUFFERING: 0
      KSQL_KSQL_SERVICE_ID: "formation_"
      KSQL_KSQL_SCHEMA_REGISTRY_URL: "http://schema-registry:8081"
      KSQL_KSQL_INTERNAL_TOPIC_REPLICAS: 3
      KSQL_KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS: 2
      KSQL_KSQL_STREAMS_REPLICATION_FACTOR: 3
      KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET: "earliest"
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_REPLICATION_FACTOR: 3
      KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
      KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
      KSQL_HEAP_OPTS: "-Xms512m -Xmx1g"
    depends_on:
      <<: *depends-on-brokers
      schema-registry:
        condition: service_started

  # ===========================================================================
  #  Profil "mirror" : cluster de secours (1 noeud combiné) + MirrorMaker 2
  # ===========================================================================
  kafka-dr:
    <<: *kafka-base
    profiles: ["mirror"]
    hostname: kafka-dr
    container_name: kafka-dr
    ports:
      - "9292:9292"
    environment:
      CLUSTER_ID: ${DR_CLUSTER_ID}
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-dr:9093"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENERS: "INTERNAL://:19092,EXTERNAL://:9292,CONTROLLER://:9093"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-dr:19092,EXTERNAL://localhost:9292"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
      KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      KAFKA_LOG_DIRS: "/var/lib/kafka/data"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m"
    volumes:
      - kafka-dr-data:/var/lib/kafka/data
    healthcheck: *broker-healthcheck

  mirror-maker:
    <<: *kafka-base
    profiles: ["mirror"]
    hostname: mirror-maker
    container_name: mirror-maker
    command: ["/opt/kafka/bin/connect-mirror-maker.sh", "/etc/mm2/mm2.properties"]
    environment:
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"
    volumes:
      - ./config/mm2/mm2.properties:/etc/mm2/mm2.properties:ro
    depends_on:
      <<: *depends-on-brokers
      kafka-dr:
        condition: service_healthy

  # ===========================================================================
  #  Profil "consoles" : AKHQ (console web alternative) : http://localhost:8090
  # ===========================================================================
  akhq:
    image: ${AKHQ_IMAGE}
    profiles: ["consoles"]
    container_name: akhq
    restart: unless-stopped
    networks:
      - kafka-net
    ports:
      - "8090:8080"
    environment:
      AKHQ_CONFIGURATION: |
        akhq:
          connections:
            kafka-kraft:
              properties:
                bootstrap.servers: "broker-1:19092,broker-2:19092,broker-3:19092"
              schema-registry:
                url: "http://schema-registry:8081"
              connect:
                - name: "connect"
                  url: "http://connect:8083"
    depends_on: *depends-on-brokers

networks:
  kafka-net:
    name: kafka-net

volumes:
  controller-1-data:
  controller-2-data:
  controller-3-data:
  broker-1-data:
  broker-2-data:
  broker-3-data:
  broker-4-data:
  kafka-dr-data:
  prometheus-data:
  grafana-data:
EOF_KAFKA_KRAFT
```

Règles de l'agent **Prometheus JMX Exporter** (métriques exposées sur le port 9404 de chaque noeud,
utilisées par Kafka UI dès maintenant et par Prometheus au TP05) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/jmx-exporter
cat > config/jmx-exporter/kafka.yml <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Règles JMX Exporter (Prometheus) pour brokers et contrôleurs Kafka KRaft
#  Source : https://github.com/prometheus/jmx_exporter (examples/kafka-kraft-3_0_0.yml)
#  Licence Apache 2.0 - utilisé par l'agent : -javaagent:...jar=9404:/opt/jmx-exporter/kafka.yml
# =============================================================================
lowercaseOutputName: true

rules:
# --- Règle ajoutée pour la formation : taux d'inactivité des threads I/O (moyenne 1 min)
- pattern: kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>OneMinuteRate
  name: kafka_server_request_handler_avg_idle_percent
  type: GAUGE

# Special cases and very specific rules
- pattern : kafka.server<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.*)><>Value
  name: kafka_server_$1_$2
  type: GAUGE
  labels:
    clientId: "$3"
    topic: "$4"
    partition: "$5"
- pattern : kafka.server<type=(.+), name=(.+), clientId=(.+), brokerHost=(.+), brokerPort=(.+)><>Value
  name: kafka_server_$1_$2
  type: GAUGE
  labels:
    clientId: "$3"
    broker: "$4:$5"
- pattern : kafka.coordinator.(\w+)<type=(.+), name=(.+)><>Value
  name: kafka_coordinator_$1_$2_$3
  type: GAUGE
# Kraft current state info metric rule
- pattern: "kafka.server<type=raft-metrics><>current-state: ([a-z]+)"
  name: kafka_server_raft_metrics_current_state_info
  type: GAUGE
  value: 1
  labels:
    "state": "$1"
# Kraft specific rules for raft-metrics, raft-channel-metrics, broker-metadata-metrics
- pattern: kafka.server<type=(.+)><>([a-z-]+)-total
  name: kafka_server_$1_$2_total
  type: COUNTER
- pattern: kafka.server<type=(.+)><>([a-z-]+)
  name: kafka_server_$1_$2
  type: GAUGE

# Generic per-second counters with 0-2 key/value pairs
- pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*, (.+)=(.+), (.+)=(.+)><>Count
  name: kafka_$1_$2_$3_total
  type: COUNTER
  labels:
    "$4": "$5"
    "$6": "$7"
- pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*, (.+)=(.+)><>Count
  name: kafka_$1_$2_$3_total
  type: COUNTER
  labels:
    "$4": "$5"
- pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*><>Count
  name: kafka_$1_$2_$3_total
  type: COUNTER

# Quota specific rules
- pattern: kafka.server<type=(.+), user=(.+), client-id=(.+)><>([a-z-]+)
  name: kafka_server_quota_$4
  type: GAUGE
  labels:
    resource: "$1"
    user: "$2"
    clientId: "$3"
- pattern: kafka.server<type=(.+), client-id=(.+)><>([a-z-]+)
  name: kafka_server_quota_$3
  type: GAUGE
  labels:
    resource: "$1"
    clientId: "$2"
- pattern: kafka.server<type=(.+), user=(.+)><>([a-z-]+)
  name: kafka_server_quota_$3
  type: GAUGE
  labels:
    resource: "$1"
    user: "$2"

# Generic gauges with 0-2 key/value pairs
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+), (.+)=(.+)><>Value
  name: kafka_$1_$2_$3
  type: GAUGE
  labels:
    "$4": "$5"
    "$6": "$7"
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+)><>Value
  name: kafka_$1_$2_$3
  type: GAUGE
  labels:
    "$4": "$5"
- pattern: kafka.(\w+)<type=(.+), name=(.+)><>Value
  name: kafka_$1_$2_$3
  type: GAUGE

# Emulate Prometheus 'Summary' metrics for the exported 'Histogram's.
#
# Note that these are missing the '_sum' metric!
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+), (.+)=(.+)><>Count
  name: kafka_$1_$2_$3_count
  type: COUNTER
  labels:
    "$4": "$5"
    "$6": "$7"
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.*), (.+)=(.+)><>(\d+)thPercentile
  name: kafka_$1_$2_$3
  type: GAUGE
  labels:
    "$4": "$5"
    "$6": "$7"
    quantile: "0.$8"
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+)><>Count
  name: kafka_$1_$2_$3_count
  type: COUNTER
  labels:
    "$4": "$5"
- pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.*)><>(\d+)thPercentile
  name: kafka_$1_$2_$3
  type: GAUGE
  labels:
    "$4": "$5"
    quantile: "0.$6"
- pattern: kafka.(\w+)<type=(.+), name=(.+)><>Count
  name: kafka_$1_$2_$3_count
  type: COUNTER
- pattern: kafka.(\w+)<type=(.+), name=(.+)><>(\d+)thPercentile
  name: kafka_$1_$2_$3
  type: GAUGE
  labels:
    quantile: "0.$4"

# Generic gauges for MeanRate Percent
# Ex) kafka.server<type=KafkaRequestHandlerPool, name=RequestHandlerAvgIdlePercent><>MeanRate
- pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*><>MeanRate
  name: kafka_$1_$2_$3_percent
  type: GAUGE
- pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*><>Value
  name: kafka_$1_$2_$3_percent
  type: GAUGE
- pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*, (.+)=(.+)><>Value
  name: kafka_$1_$2_$3_percent
  type: GAUGE
  labels:
    "$4": "$5"
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/jmx-exporter
cat > config/jmx-exporter/kafka-connect.yml <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Règles JMX Exporter (Prometheus) pour Kafka Connect / MirrorMaker 2
#  Source : https://github.com/prometheus/jmx_exporter (examples/kafka-connect.yml)
#  Licence Apache 2.0
# =============================================================================
lowercaseOutputName: true
rules:
  #kafka.connect:type=app-info,client-id="{clientid}"
  #kafka.consumer:type=app-info,client-id="{clientid}"
  #kafka.producer:type=app-info,client-id="{clientid}"
  - pattern: 'kafka.(.+)<type=app-info, client-id=(.+)><>start-time-ms'
    name: kafka_$1_start_time_seconds
    labels:
      clientId: "$2"
    help: "Kafka $1 JMX metric start time seconds"
    type: GAUGE
    valueFactor: 0.001 
  - pattern: 'kafka.(.+)<type=app-info, client-id=(.+)><>(commit-id|version): (.+)'
    name: kafka_$1_$3_info
    value: 1
    labels:
      clientId: "$2"
      $3: "$4"
    help: "Kafka $1 JMX metric info version and commit-id"
    type: GAUGE

  #kafka.producer:type=producer-topic-metrics,client-id="{clientid}",topic="{topic}"", partition="{partition}"
  #kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{clientid}",topic="{topic}"", partition="{partition}"
  - pattern: kafka.(.+)<type=(.+)-metrics, client-id=(.+), topic=(.+), partition=(.+)><>(.+-total|compression-rate|.+-avg|.+-replica|.+-lag|.+-lead)
    name: kafka_$2_$6
    labels:
      clientId: "$3"
      topic: "$4"
      partition: "$5"
    help: "Kafka $1 JMX metric type $2"
    type: GAUGE

  #kafka.producer:type=producer-topic-metrics,client-id="{clientid}",topic="{topic}"
  #kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{clientid}",topic="{topic}"", partition="{partition}"
  - pattern: kafka.(.+)<type=(.+)-metrics, client-id=(.+), topic=(.+)><>(.+-total|compression-rate|.+-avg)
    name: kafka_$2_$5
    labels:
      clientId: "$3"
      topic: "$4"
    help: "Kafka $1 JMX metric type $2"
    type: GAUGE

  #kafka.connect:type=connect-node-metrics,client-id="{clientid}",node-id="{nodeid}"
  #kafka.consumer:type=consumer-node-metrics,client-id=consumer-1,node-id="{nodeid}"
  - pattern: kafka.(.+)<type=(.+)-metrics, client-id=(.+), node-id=(.+)><>(.+-total|.+-avg)
    name: kafka_$2_$5
    labels:
      clientId: "$3"
      nodeId: "$4"
    help: "Kafka $1 JMX metric type $2"
    type: UNTYPED

  #kafka.connect:type=kafka-metrics-count,client-id="{clientid}"
  #kafka.consumer:type=consumer-fetch-manager-metrics,client-id="{clientid}"
  #kafka.consumer:type=consumer-coordinator-metrics,client-id="{clientid}"
  #kafka.consumer:type=consumer-metrics,client-id="{clientid}"
  - pattern: kafka.(.+)<type=(.+)-metrics, client-id=(.*)><>(.+-total|.+-avg|.+-bytes|.+-count|.+-ratio|.+-age|.+-flight|.+-threads|.+-connectors|.+-tasks|.+-ago)
    name: kafka_$2_$4
    labels:
      clientId: "$3"
    help: "Kafka $1 JMX metric type $2"
    type: GAUGE

  #kafka.connect:type=connector-task-metrics,connector="{connector}",task="{task}<> status"
  - pattern: 'kafka.connect<type=connector-task-metrics, connector=(.+), task=(.+)><>status: ([a-z-]+)'
    name: kafka_connect_connector_status
    value: 1
    labels:
      connector: "$1"
      task: "$2"
      status: "$3"
    help: "Kafka Connect JMX Connector status"
    type: GAUGE

  #kafka.connect:type=task-error-metrics,connector="{connector}",task="{task}"
  #kafka.connect:type=source-task-metrics,connector="{connector}",task="{task}"
  #kafka.connect:type=sink-task-metrics,connector="{connector}",task="{task}"
  #kafka.connect:type=connector-task-metrics,connector="{connector}",task="{task}"
  - pattern: kafka.connect<type=(.+)-metrics, connector=(.+), task=(.+)><>(.+-total|.+-count|.+-ms|.+-ratio|.+-avg|.+-failures|.+-requests|.+-timestamp|.+-logged|.+-errors|.+-retries|.+-skipped)
    name: kafka_connect_$1_$4
    labels:
      connector: "$2"
      task: "$3"
    help: "Kafka Connect JMX metric type $1"
    type: GAUGE

  #kafka.connect:type=connector-metrics,connector="{connector}"
  #kafka.connect:type=connect-worker-metrics,connector="{connector}"
  - pattern: kafka.connect<type=connect-worker-metrics, connector=(.+)><>([a-z-]+)
    name: kafka_connect_worker_$2
    labels:
      connector: "$1"
    help: "Kafka Connect JMX metric $1"
    type: GAUGE

  #kafka.connect:type=connect-worker-metrics
  - pattern: kafka.connect<type=connect-worker-metrics><>([a-z-]+)
    name: kafka_connect_worker_$1
    help: "Kafka Connect JMX metric worker"
    type: GAUGE

  #kafka.connect:type=connect-worker-rebalance-metrics
  - pattern: kafka.connect<type=connect-worker-rebalance-metrics><>([a-z-]+)
    name: kafka_connect_worker_rebalance_$1
    help: "Kafka Connect JMX metric rebalance information"
    type: GAUGE
EOF_KAFKA_KRAFT
```

Scripts utilitaires :

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/setup.sh <<'EOF_KAFKA_KRAFT'
#!/usr/bin/env bash
# =============================================================================
#  setup.sh - Prépare l'environnement KAFKA-KRAFT avant "docker compose up"
#   - vérifie les pré-requis (Docker, Compose, réglages système)
#   - télécharge l'agent Prometheus JMX Exporter (obligatoire : monté dans
#     tous les conteneurs Kafka via KAFKA_OPTS=-javaagent:...)
#   - crée les répertoires de données de Kafka Connect
#  Usage : ./scripts/setup.sh
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
ok "droits positionnés"

echo
echo "Environnement prêt. Démarrage du cluster :  docker compose up -d"
EOF_KAFKA_KRAFT
chmod +x scripts/setup.sh
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/kafka-env.sh <<'EOF_KAFKA_KRAFT'
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
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/cluster-status.sh <<'EOF_KAFKA_KRAFT'
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
EOF_KAFKA_KRAFT
chmod +x scripts/cluster-status.sh
```

### B.2 Préparer l'environnement

```bash
cd ~/KAFKA-KRAFT
./scripts/setup.sh
```

Affichage attendu :

```text
=== 1. Pré-requis ===
  [OK]   docker : Docker version 29.x.x ...
  [OK]   docker compose : 2.x.x
  [OK]   démon Docker joignable
  [OK]   curl présent
  [OK]   jq présent
  [OK]   vm.max_map_count = 262144
  [OK]   vm.swappiness = 1
  [OK]   mémoire WSL : 11 Go
=== 2. Agent Prometheus JMX Exporter ===
  -> téléchargement : https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.7.0/...
  [OK]   agent JMX Exporter 1.7.0 téléchargé
=== 3. Répertoires Kafka Connect ===
  [OK]   connect/data/{input,output} prêts
=== 4. Droits de lecture des fichiers montés dans les conteneurs ===
  [OK]   droits positionnés
```

> ⚠️ L'agent `config/jmx-exporter/jmx_prometheus_javaagent.jar` **doit** exister avant le démarrage :
> il est chargé par tous les noeuds Kafka (`KAFKA_OPTS=-javaagent:...`). Sans lui, la JVM ne démarre pas.

Validation de la syntaxe du fichier Compose :

```bash
docker compose config --quiet && echo "docker-compose.yml valide"
docker compose config --services
```

```text
docker-compose.yml valide
controller-1
controller-2
controller-3
broker-1
broker-2
broker-3
kafka-ui
```

> Seuls les services **sans profil** sont listés. Avec un profil : `docker compose --profile monitoring config --services`.

### B.3 Démarrer le cluster

```bash
docker compose pull
docker compose up -d
```

Suivez le démarrage (les brokers attendent que les 3 contrôleurs soient prêts, puis Kafka UI attend les
3 brokers). Comptez 1 à 2 minutes ; Ctrl+C quand tout est `healthy` :

```bash
watch -n 5 'docker compose ps --format "table {{.Name}}\t{{.Status}}"'
```

```text
NAME           STATUS
broker-1       Up 2 minutes (healthy)
broker-2       Up 2 minutes (healthy)
broker-3       Up 2 minutes (healthy)
controller-1   Up 2 minutes (healthy)
controller-2   Up 2 minutes (healthy)
controller-3   Up 2 minutes (healthy)
kafka-ui       Up 1 minute
```

Les logs :

```bash
docker compose logs controller-1 | grep -iE "formatting|elected|leader" | head
docker compose logs broker-1 | grep -E "Kafka Server started|Transitioning from RECOVERY to RUNNING"
```

> Le conteneur formate automatiquement son stockage au **premier** démarrage avec `CLUSTER_ID`
> (équivalent de `kafka-storage.sh format`), puis réutilise le volume Docker aux démarrages suivants.

Configuration **réellement utilisée** par un broker (générée à partir des variables `KAFKA_*`) :

```bash
docker exec broker-1 cat /opt/kafka/config/server.properties | sort
```

### B.4 Explorer le cluster

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-cluster.sh cluster-id --bootstrap-server $BOOTSTRAP
kafka-cluster.sh list-endpoints --bootstrap-server $BOOTSTRAP
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```

Affichage attendu (extraits ; le contrôleur leader peut être 1, 2 ou 3) :

```text
Cluster ID: PIYzDzl8TomQ84Cnrxi0gA

ID         HOST      PORT       RACK       ENDPOINT_TYPE
11         localhost 9092       rack-a     broker
12         localhost 9094       rack-b     broker
13         localhost 9096       rack-c     broker

ClusterId:              PIYzDzl8TomQ84Cnrxi0gA
LeaderId:               2
LeaderEpoch:            1
HighWatermark:          ...
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://controller-1:9093"]}, {"id": 2, ...}, {"id": 3, ...}]
CurrentObservers:       [{"id": 11, ...}, {"id": 12, ...}, {"id": 13, ...}]

NodeId  DirectoryId             LogEndOffset  Lag  LastFetchTimestamp  LastCaughtUpTimestamp  Status
2       ...                     1234          0    ...                 ...                    Leader
1       ...                     1234          0    ...                 ...                    Follower
3       ...                     1234          0    ...                 ...                    Follower
11      ...                     1234          0    ...                 ...                    Observer
12      ...                     1234          0    ...                 ...                    Observer
13      ...                     1234          0    ...                 ...                    Observer
```

- Les **voters** (contrôleurs) votent pour élire le leader du quorum
- Les brokers sont des **observers** : ils répliquent le journal des métadonnées sans voter

Ouvrez **Kafka UI** depuis le navigateur Windows : <http://localhost:8080> → cluster `kafka-kraft`
→ *Brokers* (3 brokers, racks, métriques), *Topics*, *Consumers*.

> Le second cluster `kafka-dr` apparaît *offline* : il n'est démarré qu'au TP07e (MirrorMaker 2).

Etat synthétique à tout moment :

```bash
~/KAFKA-KRAFT/scripts/cluster-status.sh
```

---

## Partie C – Administration des topics

### C.1 Création, valeurs par défaut et description

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic test-defauts
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic test-defauts
```

```text
Created topic test-defauts.
Topic: test-defauts	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	Configs: min.insync.replicas=2,segment.bytes=1073741824
	Topic: test-defauts	Partition: 0	Leader: 12	Replicas: 12,13,11	Isr: 12,13,11	Elr: 	LastKnownElr: 
	Topic: test-defauts	Partition: 1	Leader: 13	Replicas: 13,11,12	Isr: 13,11,12	Elr: 	LastKnownElr: 
	Topic: test-defauts	Partition: 2	Leader: 11	Replicas: 11,12,13	Isr: 11,12,13	Elr: 	LastKnownElr: 
```

Lecture : 3 partitions (valeur par défaut des contrôleurs), répliquées 3 fois ; le **premier** réplica de
la liste est le **leader préféré** ; les leaders sont répartis sur les 3 brokers (équilibrage de charge) ;
`Isr` = réplicas synchronisés ; `Elr` = *Eligible Leader Replicas* (KIP-966, Kafka 4.x).

Création explicite des topics des TP (script déclaratif et **idempotent** `--if-not-exists`) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/topics
cat > config/topics/topics.csv <<'EOF_KAFKA_KRAFT'
# nom;partitions;facteur_de_réplication;configs (k=v séparés par des virgules)
test-formation;3;3;
orders;6;3;
orders-dlt;6;3;
orders-audit;3;3;
orders-big;3;3;
orders-revenue-by-country;3;3;
transactions-in;3;3;
transactions-out;3;3;
words-input;3;3;
words-output;3;3;
streams-input;3;3;
streams-output;3;3;
films;1;3;
films-json;1;3;
customers-compacted;1;3;cleanup.policy=compact,segment.ms=10000,min.cleanable.dirty.ratio=0.01,delete.retention.ms=1000,min.compaction.lag.ms=0
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/create-topics.sh <<'EOF_KAFKA_KRAFT'
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
EOF_KAFKA_KRAFT
chmod +x scripts/create-topics.sh
```

```bash
~/KAFKA-KRAFT/scripts/create-topics.sh
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list --exclude-internal
```

```text
[OK] test-formation (partitions=3, RF=3)
[OK] orders (partitions=6, RF=3)
...
[OK] customers-compacted (partitions=1, RF=3, cleanup.policy=compact,segment.ms=10000,...)
```

### C.2 Placement explicite des réplicas

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic placement-explicite --replica-assignment "13:12,11:12"
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic placement-explicite
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic placement-ko --replica-assignment "13:14,14:12"
```

```text
	Topic: placement-explicite	Partition: 0	Leader: 13	Replicas: 13,12	Isr: 13,12 ...
	Topic: placement-explicite	Partition: 1	Leader: 11	Replicas: 11,12	Isr: 11,12 ...
Error while executing topic command : The manual partition assignment includes broker 14, but no such broker is registered.
```

### C.3 Configuration d'un topic, topics internes, suppression

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name test-defauts --alter --add-config retention.ms=3600000
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name test-defauts --describe
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topics-with-overrides
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep '^__'
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic test-defauts
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic placement-explicite
```

```text
Completed updating config for topic test-defauts.
Dynamic configs for topic test-defauts are:
  retention.ms=3600000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=3600000}
...
__consumer_offsets
```

> `__consumer_offsets` (50 partitions) est créé au premier usage d'un consumer group ;
> `__transaction_state` au premier producer transactionnel ; `__share_group_state` au premier share group.

---

## Partie D – Partitionnement et réplication

### D.1 Répartition des messages par clé

```bash
for i in $(seq 1 12); do echo "client-$((i % 4)):commande $i"; done | \
  kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic test-formation \
  --reader-property parse.key=true --reader-property key.separator=:
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic test-formation --from-beginning --timeout-ms 5000 \
  --formatter-property print.partition=true --formatter-property print.key=true | sort
```

```text
Partition:0	client-1	commande 1
Partition:0	client-1	commande 5
Partition:0	client-1	commande 9
Partition:1	client-0	commande 12
...
Processed a total of 12 messages
```

Même clé ⇒ même partition ⇒ ordre préservé pour cette clé.

### D.2 Réplication visible sur disque

```bash
docker exec broker-1 ls /var/lib/kafka/data | grep test-formation
docker exec broker-2 ls /var/lib/kafka/data | grep test-formation
```

Chaque broker possède un répertoire par partition **dont il détient un réplica** (leader ou follower).

---

## Partie E – Tolérance aux pannes

### E.1 Préparer un producteur continu (`acks=all`)

Topic de test et producteur à 10 messages/s dans un **terminal dédié** (laissez-le tourner) :

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic resilience --partitions 3 --replication-factor 3
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic resilience \
  --num-records 1000000 --record-size 100 --throughput 10 --reporting-interval 5000 \
  --command-property acks=all delivery.timeout.ms=15000 request.timeout.ms=5000
```

```text
50 records sent, 10.0 records/sec (0.00 MB/sec), 4.2 ms avg latency, ...
```

> Avec `kafka-producer-perf-test.sh`, toutes les propriétés se passent **après un seul**
> `--command-property` (séparées par des espaces) ; répéter l'option ne garde que la dernière.

### E.2 Panne d'un broker

Dans un autre terminal :

```bash
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml stop broker-2
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic resilience
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions
```

```text
	Topic: resilience	Partition: 0	Leader: 13	Replicas: 12,13,11	Isr: 13,11	...
	Topic: resilience	Partition: 1	Leader: 13	Replicas: 13,11,12	Isr: 13,11	...
	Topic: resilience	Partition: 2	Leader: 11	Replicas: 11,12,13	Isr: 11,13	...
```

- Le broker 12 a quitté l'ISR ; les partitions dont il était leader ont élu un **nouveau leader** dans l'ISR
- Le producteur **continue sans erreur** : 2 réplicas in-sync ≥ `min.insync.replicas=2`
- Kafka UI → *Brokers* : 2 brokers en ligne, partitions sous-répliquées signalées

### E.3 Panne d'un second broker : `min.insync.replicas` en action

```bash
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml stop broker-3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-min-isr-partitions
```

Après quelques secondes, le producteur `acks=all` affiche des erreurs :

```text
org.apache.kafka.common.errors.TimeoutException: Expiring 12 record(s) for resilience-1:15003 ms has passed since batch creation
  (cause : NOT_ENOUGH_REPLICAS - les écritures sont refusées tant que l'ISR < 2)
```

Un producteur en `acks=1` peut encore écrire (au risque de **perdre** ces messages si le dernier broker tombe) :

```bash
echo "message acks=1" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic resilience --request-required-acks 1
```

> **Compromis disponibilité / durabilité** : `acks=all` + `min.insync.replicas=2` + RF=3 =
> aucune perte de données acquittées tant qu'au plus 1 broker est perdu.

### E.4 Retour à la normale et élection du leader préféré

```bash
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml start broker-2 broker-3
sleep 30
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic resilience
```

Le producteur reprend ; l'ISR redevient complet, mais **tous les leaders sont sur le broker 11** :

```text
	Topic: resilience	Partition: 0	Leader: 11	Replicas: 12,13,11	Isr: 11,12,13
	Topic: resilience	Partition: 1	Leader: 11	Replicas: 13,11,12	Isr: 11,13,12
	Topic: resilience	Partition: 2	Leader: 11	Replicas: 11,12,13	Isr: 11,12,13
```

Rééquilibrage des leaders (sinon automatique au bout de 5 min avec `auto.leader.rebalance.enable=true`) :

```bash
kafka-leader-election.sh --bootstrap-server $BOOTSTRAP --election-type PREFERRED --all-topic-partitions
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic resilience
```

```text
Successfully completed leader election (PREFERRED) for partitions resilience-0, resilience-1, ...
	Topic: resilience	Partition: 0	Leader: 12	Replicas: 12,13,11	...
	Topic: resilience	Partition: 1	Leader: 13	Replicas: 13,11,12	...
	Topic: resilience	Partition: 2	Leader: 11	Replicas: 11,12,13	...
```

### E.5 Panne de contrôleurs : le quorum KRaft

Identifiez le contrôleur **actif** puis arrêtez-le :

```bash
LEADER=$(kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status | awk '/LeaderId/ {print $2}')
echo "Contrôleur actif : controller-$LEADER"
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml stop controller-$LEADER
sleep 10
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status | grep -E "LeaderId|LeaderEpoch"
```

Un **nouveau leader** est élu (l'*epoch* augmente) : le cluster reste pleinement opérationnel (2/3 voters).

Arrêtez maintenant un **second** contrôleur (le quorum est perdu : 1/3 voters) :

```bash
OTHER=$(kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status | awk '/LeaderId/ {print $2}')
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml stop controller-$OTHER
printf 'default.api.timeout.ms=15000\nrequest.timeout.ms=10000\n' > /tmp/admin-timeout.properties
kafka-topics.sh --bootstrap-server $BOOTSTRAP --command-config /tmp/admin-timeout.properties --create --topic sans-quorum
```

- La création de topic **échoue** (`TimeoutException`) : plus de contrôleur actif pour modifier les métadonnées
- **Mais le producteur du terminal E.1 continue d'écrire** : les brokers servent les données avec les
  dernières métadonnées connues. Le quorum ne concerne que les **métadonnées**.

Redémarrez les contrôleurs :

```bash
docker compose -f ~/KAFKA-KRAFT/docker-compose.yml start controller-1 controller-2 controller-3
sleep 15
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status | grep -E "LeaderId|CurrentVoters"
```

Arrêtez le producteur du terminal E.1 (Ctrl+C).

> **Election "unclean"** : si tous les réplicas de l'ISR sont perdus, `unclean.leader.election.enable=true`
> autoriserait un réplica en retard à devenir leader (disponibilité retrouvée **mais** messages perdus).
> Laisser à `false` en production ; en cas d'urgence : `kafka-leader-election.sh --election-type UNCLEAN`.

---

## Partie F – Scalabilité

### F.1 Ajouter des partitions : impact sur la répartition par clé (ex-TP03b)

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic scalabilite --partitions 1 --replication-factor 3
printf 'cle1:enreg1\ncle2:enreg2\ncle3:enreg3\ncle4:enreg4\n' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP \
  --topic scalabilite --reader-property parse.key=true --reader-property key.separator=:
kafka-topics.sh --bootstrap-server $BOOTSTRAP --alter --topic scalabilite --partitions 3
printf 'cle1:enreg11\ncle2:enreg21\ncle3:enreg31\ncle4:enreg41\n' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP \
  --topic scalabilite --reader-property parse.key=true --reader-property key.separator=:
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic scalabilite --from-beginning --timeout-ms 5000 \
  --formatter-property print.partition=true --formatter-property print.key=true | sort
```

```text
Partition:0	cle1	enreg1
Partition:0	cle2	enreg2
Partition:0	cle3	enreg3
Partition:0	cle4	enreg4
Partition:1	cle1	enreg11      <- la clé cle1 a changé de partition !
...
```

> On ne peut **qu'augmenter** le nombre de partitions, et cela **casse l'affinité clé → partition**
> (ordre par clé non garanti pendant la transition). Bien dimensionner dès la création.

### F.2 Ajouter un broker

```bash
cd ~/KAFKA-KRAFT
docker compose --profile scale up -d broker-4
sleep 40
kafka-cluster.sh list-endpoints --bootstrap-server $BOOTSTRAP
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders | grep -c 'Replicas: [0-9,]*14' || true
```

Le broker 14 est enregistré... mais **ne porte aucune partition** (0) : Kafka ne déplace jamais les
données automatiquement (outils tiers : Cruise Control, Confluent Self-Balancing Clusters).

### F.3 Réaffecter les partitions sur 4 brokers (avec limitation de débit)

```bash
cd /tmp
echo '{"version":1,"topics":[{"topic":"orders"},{"topic":"test-formation"}]}' > topics-to-move.json
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --topics-to-move-json-file topics-to-move.json \
  --broker-list "11,12,13,14" --generate > reassign-plan.txt
cat reassign-plan.txt
sed -n '/^Current partition replica assignment/{n;p}' reassign-plan.txt > rollback.json
sed -n '/^Proposed partition reassignment configuration/{n;p}' reassign-plan.txt > reassignment.json
```

Exécution avec un débit de réplication limité à 1 Mo/s (protège les clients pendant le déplacement) :

```bash
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file reassignment.json --execute --throttle 1048576
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --describe | grep -E "Dynamic configs|throttled.rate"
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file reassignment.json --verify
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders
```

```text
Successfully started partition reassignments for orders-0,orders-1,...
  leader.replication.throttled.rate=1048576 ...
Status of partition reassignment:
Reassignment of partition orders-0 is completed.
...
Clearing broker-level throttles on brokers 11,12,13,14
Clearing topic-level throttles on topic orders
```

> Relancez `--verify` jusqu'à ce que toutes les réaffectations soient terminées : c'est lui qui
> **supprime les limitations de débit**. Oublier cette étape = réplication bridée indéfiniment !

### F.4 Retirer le broker (décommissionnement)

```bash
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file rollback.json --execute
sleep 10
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file rollback.json --verify
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe | grep -c 'Replicas: [0-9,]*14' || true
cd ~/KAFKA-KRAFT && docker compose --profile scale stop broker-4
sleep 15
kafka-cluster.sh unregister --bootstrap-server $BOOTSTRAP --id 14
kafka-cluster.sh list-endpoints --bootstrap-server $BOOTSTRAP
```

Le comptage doit renvoyer `0` avant l'arrêt. Après `unregister`, le broker 14 n'apparaît plus.

---

## Partie G – Fin du TP

On garde le cluster démarré pour la suite. Pour l'arrêter / le redémarrer :

```bash
cd ~/KAFKA-KRAFT
docker compose stop          # arrêt (données conservées dans les volumes)
docker compose up -d         # redémarrage
./scripts/stop-all.sh --purge  # suppression complète (tous profils, volumes inclus)
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/stop-all.sh <<'EOF_KAFKA_KRAFT'
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
EOF_KAFKA_KRAFT
chmod +x scripts/stop-all.sh
```

### Ce qu'il faut retenir

1. Production : **contrôleurs dédiés** (3 ou 5) + brokers ; **RF=3, min.insync.replicas=2, acks=all**.
2. `advertised.listeners` = adresses que les clients doivent pouvoir joindre (un listener par réseau).
3. Perte d'un broker : nouveaux leaders élus dans l'ISR, aucune perte avec `acks=all`.
4. Perte du quorum de contrôleurs : plus de changement de métadonnées, mais les données restent servies.
5. Ajout de partitions = rupture de l'affinité clé/partition ; ajout de brokers = **réaffectation manuelle**
   (`kafka-reassign-partitions.sh` + `--throttle` + `--verify`).

### Questions de révision

1. Pourquoi un quorum de 4 contrôleurs n'apporte-t-il rien par rapport à 3 ?
2. Un topic RF=3, `min.insync.replicas=3` : combien de brokers peut-on perdre en écriture `acks=all` ?
3. Quelle différence entre `listeners` et `advertised.listeners` ?
4. Que se passe-t-il si l'on oublie `--verify` après une réaffectation avec `--throttle` ?

✅ **Suite : TP03 – Configurer et utiliser les API Producer et Consumer.**
