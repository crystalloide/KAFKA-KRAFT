# TP05 – Superviser et optimiser un cluster Kafka (Prometheus / Grafana)

> **Chapitre 5 du programme** – Objectif pédagogique n°4 (exploitation)
> Durée : 1 h 30
> Pré-requis : cluster du TP02 démarré, agent JMX Exporter téléchargé (`scripts/setup.sh`)
> Reprend et met à jour : TP24a (JMX), TP28 (benchmark), TP39a/b (Prometheus/Grafana)

---

## Objectifs

- Connaître les **métriques** essentielles et leurs **seuils** d'alerte
- Mettre en place une chaîne de supervision **JMX Exporter → Prometheus → Grafana** (+ alertes)
- Mesurer le **lag** des consumer groups
- Réaliser un **benchmark** et optimiser producteurs, consommateurs et brokers
- Savoir **dimensionner** un cluster et appliquer le **tuning** logiciel et système

---

## Partie A – Rappels

### A.1 Chaîne de supervision du projet

```text
 controller-1..3, broker-1..3, connect        kafka-exporter              Spring Boot (TP03)
 agent JMX Exporter :9404/metrics             :9308 (lag des groupes)     :8085/actuator/prometheus
              │                                     │                            │
              └───────────────┬─────────────────────┴────────────────────────────┘
                              ▼  scrape toutes les 15 s
                       Prometheus :9090  ── règles d'alerte (alerts.yml)
                              ▼
                       Grafana :3000  (tableau de bord "Kafka KRaft - Vue d'ensemble")
```

**Quels outils pour collecter les métriques ?**

| Outil | Rôle |
|---|---|
| **JMX** (MBeans) | Source native : tous les composants Kafka (brokers, contrôleurs, clients, Connect, Streams) |
| **Prometheus JMX Exporter** (agent Java) | Expose les MBeans au format Prometheus (ce TP) |
| **kafka-exporter** / Burrow | Lag des consumer groups calculé depuis les offsets |
| Prometheus + Alertmanager / Grafana | Stockage, alertes, tableaux de bord |
| Kafka UI, AKHQ | Vision « exploitation » instantanée |
| Cruise Control | Analyse de charge et rééquilibrage automatique |
| Solutions du marché | Datadog, Dynatrace, Zabbix, Confluent Control Center... |

### A.2 Quelles métriques et quels seuils surveiller ?

| Domaine | Métrique (MBean) | Nom Prometheus (ce projet) | Seuil / attendu |
|---|---|---|---|
| Disponibilité | `KafkaController/ActiveControllerCount` (contrôleurs) | `kafka_controller_kafkacontroller_activecontrollercount` | somme = **1** |
| Disponibilité | `KafkaController/OfflinePartitionsCount` | `kafka_controller_kafkacontroller_offlinepartitionscount` | **0** (critique) |
| Disponibilité | `KafkaController/ActiveBrokerCount`, `FencedBrokerCount` | `..._activebrokercount`, `..._fencedbrokercount` | = nb brokers / 0 |
| Réplication | `ReplicaManager/UnderReplicatedPartitions` | `kafka_server_replicamanager_underreplicatedpartitions` | **0** (alerte si > 0 pendant 2 min) |
| Réplication | `ReplicaManager/UnderMinIsrPartitionCount` | `kafka_server_replicamanager_underminisrpartitioncount` | **0** (critique : écritures `acks=all` refusées) |
| Réplication | `ReplicaManager/IsrShrinksPerSec`, `IsrExpandsPerSec` | `..._isrshrinks_total`, `..._isrexpands_total` | ~0 en régime établi |
| Saturation | `KafkaRequestHandlerPool/RequestHandlerAvgIdlePercent` | `kafka_server_request_handler_avg_idle_percent` | > **0,3** |
| Saturation | `SocketServer/NetworkProcessorAvgIdlePercent` | `kafka_network_socketserver_networkprocessoravgidlepercent` | > **0,3** |
| Latence | `RequestMetrics/TotalTimeMs` (p99, `request=Produce/FetchConsumer`) | `kafka_network_requestmetrics_totaltimems{quantile="0.99"}` | selon SLA (ex. < 100 ms) |
| Débit | `BrokerTopicMetrics/MessagesInPerSec`, `BytesInPerSec`, `BytesOutPerSec` | `kafka_server_brokertopicmetrics_*_total` | tendance, pics |
| Consommateurs | lag par groupe/partition | `kafka_consumergroup_lag` (kafka-exporter) | selon SLA (alerte > 1000 ici) |
| KRaft | `raft-metrics` : `current-state`, `commit-latency-avg` | `kafka_server_raft_metrics_current_state_info` | 1 leader |
| JVM | heap, GC | `jvm_memory_used_bytes`, `jvm_gc_*` | heap < 85 % |
| Système | disque, CPU, réseau, descripteurs | node-exporter (non déployé ici) | disque < 70-80 % |

---

## Partie B – Mise en place de Prometheus et Grafana

### B.1 Fichiers de configuration

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/prometheus
cat > config/prometheus/prometheus.yml <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Prometheus - collecte des métriques du cluster Kafka KRaft
# =============================================================================
global:
  scrape_interval: 15s
  scrape_timeout: 10s
  evaluation_interval: 15s
  external_labels:
    environment: formation

rule_files:
  - /etc/prometheus/alerts.yml

scrape_configs:
  # Prometheus lui-même
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]

  # Contrôleurs KRaft (agent JMX Exporter, port 9404)
  - job_name: kafka-controllers
    static_configs:
      - targets: ["controller-1:9404", "controller-2:9404", "controller-3:9404"]
        labels:
          role: controller
    relabel_configs:
      - source_labels: [__address__]
        regex: "([^:]+):.*"
        target_label: instance
        replacement: "$1"

  # Brokers (agent JMX Exporter, port 9404)
  - job_name: kafka-brokers
    static_configs:
      - targets: ["broker-1:9404", "broker-2:9404", "broker-3:9404"]
        labels:
          role: broker
    relabel_configs:
      - source_labels: [__address__]
        regex: "([^:]+):.*"
        target_label: instance
        replacement: "$1"

  # Broker supplémentaire (profil "scale") : DOWN tant que broker-4 n'est pas démarré
  - job_name: kafka-broker-scale
    static_configs:
      - targets: ["broker-4:9404"]
        labels:
          role: broker
    relabel_configs:
      - source_labels: [__address__]
        regex: "([^:]+):.*"
        target_label: instance
        replacement: "$1"

  # Kafka Connect (profil "connect")
  - job_name: kafka-connect
    static_configs:
      - targets: ["connect:9404"]
        labels:
          role: connect
    relabel_configs:
      - source_labels: [__address__]
        regex: "([^:]+):.*"
        target_label: instance
        replacement: "$1"

  # Lag des consumer groups (kafka-exporter)
  - job_name: kafka-exporter
    static_configs:
      - targets: ["kafka-exporter:9308"]

  # Application Spring Boot du TP (lancée sous WSL : mvn spring-boot:run)
  - job_name: spring-kafka-demo
    metrics_path: /actuator/prometheus
    static_configs:
      - targets: ["host.docker.internal:8085"]
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/prometheus
cat > config/prometheus/alerts.yml <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Règles d'alerte Prometheus - seuils recommandés pour un cluster Kafka KRaft
#  (visibles dans http://localhost:9090/alerts)
# =============================================================================
groups:
  - name: kafka-cluster
    rules:
      - alert: KafkaBrokerDown
        expr: up{job="kafka-brokers"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Broker {{ $labels.instance }} injoignable"
          description: "L'agent JMX du broker {{ $labels.instance }} ne répond plus depuis 1 minute."

      - alert: KafkaControllerDown
        expr: up{job="kafka-controllers"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Contrôleur {{ $labels.instance }} injoignable"
          description: "Le quorum KRaft tolère la perte de (N-1)/2 contrôleurs : 1 sur 3."

      - alert: KafkaActiveControllerCountNot1
        expr: sum(kafka_controller_kafkacontroller_activecontrollercount) != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nombre de contrôleurs actifs = {{ $value }} (attendu : 1)"
          description: "0 = quorum perdu (plus de leader Raft), >1 = situation anormale."

      - alert: KafkaOfflinePartitions
        expr: max(kafka_controller_kafkacontroller_offlinepartitionscount) > 0
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "{{ $value }} partition(s) hors ligne (sans leader)"
          description: "Des partitions n'ont plus de leader : production et consommation impossibles."

      - alert: KafkaUnderMinIsrPartitions
        expr: sum(kafka_server_replicamanager_underminisrpartitioncount) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "{{ $value }} partition(s) sous min.insync.replicas"
          description: "Les producteurs en acks=all reçoivent NotEnoughReplicas."

      - alert: KafkaUnderReplicatedPartitions
        expr: sum(kafka_server_replicamanager_underreplicatedpartitions) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} partition(s) sous-répliquée(s)"
          description: "Au moins un réplica n'est plus dans l'ISR (broker arrêté, disque ou réseau lent)."

      - alert: KafkaFencedBrokers
        expr: max(kafka_controller_kafkacontroller_fencedbrokercount) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "{{ $value }} broker(s) isolé(s) (fenced) par le contrôleur"

      - alert: KafkaRequestHandlerSaturation
        expr: kafka_server_request_handler_avg_idle_percent < 0.3
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Threads I/O du broker {{ $labels.instance }} saturés (idle < 30 %)"
          description: "Envisager d'augmenter num.io.threads ou d'ajouter des brokers."

      - alert: KafkaNetworkProcessorSaturation
        expr: kafka_network_socketserver_networkprocessoravgidlepercent < 0.3
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Threads réseau du broker {{ $labels.instance }} saturés (idle < 30 %)"
          description: "Envisager d'augmenter num.network.threads."

      - alert: KafkaJvmHeapHigh
        expr: jvm_memory_used_bytes{area="heap"} / jvm_memory_max_bytes{area="heap"} > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Heap JVM de {{ $labels.instance }} > 85 %"

  - name: kafka-consumers
    rules:
      - alert: KafkaConsumerGroupLagHigh
        expr: sum by (consumergroup, topic) (kafka_consumergroup_lag) > 1000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Lag de {{ $value }} messages pour le groupe {{ $labels.consumergroup }} sur {{ $labels.topic }}"
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/grafana/provisioning/datasources
cat > config/grafana/provisioning/datasources/datasource.yml <<'EOF_KAFKA_KRAFT'
# Source de données Prometheus provisionnée automatiquement
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
    jsonData:
      timeInterval: 15s
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/grafana/provisioning/dashboards
cat > config/grafana/provisioning/dashboards/dashboards.yml <<'EOF_KAFKA_KRAFT'
# Tableaux de bord provisionnés automatiquement depuis /etc/grafana/dashboards
apiVersion: 1

providers:
  - name: kafka-kraft
    orgId: 1
    folder: Kafka
    type: file
    disableDeletion: false
    allowUiUpdates: true
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/dashboards
      foldersFromFilesStructure: false
EOF_KAFKA_KRAFT
```

Tableau de bord Grafana (provisionné automatiquement) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/grafana/dashboards
cat > config/grafana/dashboards/kafka-overview.json <<'EOF_KAFKA_KRAFT'
{
  "uid": "kafka-kraft-overview",
  "title": "Kafka KRaft - Vue d'ensemble",
  "tags": [
    "kafka",
    "kraft",
    "formation"
  ],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "editable": true,
  "graphTooltip": 1,
  "refresh": "10s",
  "time": {
    "from": "now-30m",
    "to": "now"
  },
  "timepicker": {},
  "templating": {
    "list": []
  },
  "annotations": {
    "list": []
  },
  "panels": [
    {
      "id": 1,
      "type": "row",
      "title": "Santé du cluster",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 0
      },
      "panels": []
    },
    {
      "id": 2,
      "type": "stat",
      "title": "Brokers actifs",
      "description": "ActiveBrokerCount vu par le contrôleur actif (attendu : 3)",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 0,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max(kafka_controller_kafkacontroller_activebrokercount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "red",
                "value": null
              },
              {
                "color": "orange",
                "value": 2
              },
              {
                "color": "green",
                "value": 3
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 3,
      "type": "stat",
      "title": "Contrôleur actif",
      "description": "Doit valoir exactement 1",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 3,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum(kafka_controller_kafkacontroller_activecontrollercount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "red",
                "value": null
              },
              {
                "color": "green",
                "value": 1
              },
              {
                "color": "red",
                "value": 2
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 4,
      "type": "stat",
      "title": "Brokers isolés (fenced)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 6,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max(kafka_controller_kafkacontroller_fencedbrokercount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 1
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 5,
      "type": "stat",
      "title": "Partitions hors ligne",
      "description": "Seuil : 0",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 9,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max(kafka_controller_kafkacontroller_offlinepartitionscount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 1
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 6,
      "type": "stat",
      "title": "Sous-répliquées",
      "description": "UnderReplicatedPartitions - seuil : 0",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 12,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum(kafka_server_replicamanager_underreplicatedpartitions)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "orange",
                "value": 1
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 7,
      "type": "stat",
      "title": "Sous min.insync.replicas",
      "description": "Seuil : 0 (sinon NotEnoughReplicas en acks=all)",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 15,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum(kafka_server_replicamanager_underminisrpartitioncount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              },
              {
                "color": "red",
                "value": 1
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 8,
      "type": "stat",
      "title": "Topics",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 18,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max(kafka_controller_kafkacontroller_globaltopiccount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 9,
      "type": "stat",
      "title": "Partitions",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 4,
        "w": 3,
        "x": 21,
        "y": 1
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max(kafka_controller_kafkacontroller_globalpartitioncount)",
          "refId": "A",
          "instant": true
        }
      ],
      "fieldConfig": {
        "defaults": {
          "thresholds": {
            "mode": "absolute",
            "steps": [
              {
                "color": "green",
                "value": null
              }
            ]
          },
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "reduceOptions": {
          "calcs": [
            "lastNotNull"
          ],
          "fields": "",
          "values": false
        },
        "colorMode": "background",
        "graphMode": "none",
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": "auto"
      }
    },
    {
      "id": 10,
      "type": "row",
      "title": "Débits",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 5
      },
      "panels": []
    },
    {
      "id": 11,
      "type": "timeseries",
      "title": "Messages entrants / s (par broker)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 6
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum by (instance) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"\"}[1m]))",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 12,
      "type": "timeseries",
      "title": "Octets entrants / sortants / s (cluster)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 6
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"\"}[1m]))",
          "legendFormat": "entrant",
          "refId": "A"
        },
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum(rate(kafka_server_brokertopicmetrics_bytesout_total{topic=\"\"}[1m]))",
          "legendFormat": "sortant",
          "refId": "B"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "Bps",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 13,
      "type": "timeseries",
      "title": "Messages entrants / s (par topic)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 14
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=\"\"}[1m]))",
          "legendFormat": "{{topic}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 14,
      "type": "timeseries",
      "title": "Latence p99 des requêtes (ms)",
      "description": "Produce = écriture, FetchConsumer = lecture client, Fetch = réplication",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 14
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "max by (request) (kafka_network_requestmetrics_totaltimems{request=~\"Produce|FetchConsumer|Fetch\",quantile=\"0.99\"})",
          "legendFormat": "{{request}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "ms",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 15,
      "type": "row",
      "title": "Saturation et réplication",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 22
      },
      "panels": []
    },
    {
      "id": 16,
      "type": "timeseries",
      "title": "Inactivité des threads I/O (RequestHandlerAvgIdle)",
      "description": "Alerte si < 30 %",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 0,
        "y": 23
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "kafka_server_request_handler_avg_idle_percent",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percentunit",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 17,
      "type": "timeseries",
      "title": "Inactivité des threads réseau (NetworkProcessorAvgIdle)",
      "description": "Alerte si < 30 %",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 8,
        "y": 23
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "kafka_network_socketserver_networkprocessoravgidlepercent",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percentunit",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 18,
      "type": "timeseries",
      "title": "ISR shrink / expand (par minute)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 16,
        "y": 23
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum by (instance) (rate(kafka_server_replicamanager_isrshrinks_total[5m])) * 60",
          "legendFormat": "shrink {{instance}}",
          "refId": "A"
        },
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum by (instance) (rate(kafka_server_replicamanager_isrexpands_total[5m])) * 60",
          "legendFormat": "expand {{instance}}",
          "refId": "B"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 19,
      "type": "timeseries",
      "title": "Partitions par broker",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 0,
        "y": 31
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "kafka_server_replicamanager_partitioncount",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 20,
      "type": "timeseries",
      "title": "Partitions leader par broker",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 8,
        "y": 31
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "kafka_server_replicamanager_leadercount",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 21,
      "type": "timeseries",
      "title": "Rôle Raft des contrôleurs (1 = leader)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 8,
        "x": 16,
        "y": 31
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "kafka_server_raft_metrics_current_state_info{role=\"controller\",state=\"leader\"}",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 22,
      "type": "row",
      "title": "Consommateurs et JVM",
      "collapsed": false,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 39
      },
      "panels": []
    },
    {
      "id": 23,
      "type": "timeseries",
      "title": "Lag des consumer groups (messages)",
      "description": "Source : kafka-exporter",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 40
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "sum by (consumergroup, topic) (kafka_consumergroup_lag)",
          "legendFormat": "{{consumergroup}} / {{topic}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    },
    {
      "id": 24,
      "type": "timeseries",
      "title": "Heap JVM utilisée (%)",
      "description": "",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 40
      },
      "targets": [
        {
          "datasource": {
            "type": "prometheus",
            "uid": "prometheus"
          },
          "expr": "jvm_memory_used_bytes{area=\"heap\"} / jvm_memory_max_bytes{area=\"heap\"}",
          "legendFormat": "{{instance}}",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percentunit",
          "custom": {
            "drawStyle": "line",
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never"
          }
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom",
          "showLegend": true
        },
        "tooltip": {
          "mode": "multi",
          "sort": "none"
        }
      }
    }
  ]
}
EOF_KAFKA_KRAFT
```

### B.2 Démarrer le profil `monitoring`

```bash
cd ~/KAFKA-KRAFT
chmod -R a+rX config
docker compose --profile monitoring up -d
docker compose --profile monitoring ps --format "table {{.Name}}\t{{.Status}}"
```

```text
NAME             STATUS
...
grafana          Up 20 seconds
kafka-exporter   Up 20 seconds
prometheus       Up 20 seconds
```

### B.3 Vérifier la collecte

Les métriques brutes d'un broker (format texte Prometheus) :

```bash
docker exec broker-1 wget -qO- http://localhost:9404/metrics | grep -E '^kafka_server_replicamanager_(underreplicatedpartitions|partitioncount|leadercount)'
```

```text
kafka_server_replicamanager_leadercount 25.0
kafka_server_replicamanager_partitioncount 75.0
kafka_server_replicamanager_underreplicatedpartitions 0.0
```

Prometheus : <http://localhost:9090/targets> → tous les *targets* `kafka-controllers`, `kafka-brokers`,
`kafka-exporter` sont **UP** (`kafka-broker-scale`, `kafka-connect` et `spring-kafka-demo` sont DOWN tant
que les composants correspondants ne sont pas démarrés : c'est normal).

Requêtes PromQL à tester dans <http://localhost:9090/query> (ou en ligne de commande) :

```bash
q() { curl -s --get localhost:9090/api/v1/query --data-urlencode "query=$1" | jq -r '.data.result[] | "\(.metric.instance // .metric.consumergroup // "-")\t\(.value[1])"'; }
q 'sum(kafka_controller_kafkacontroller_activecontrollercount)'
q 'max(kafka_controller_kafkacontroller_activebrokercount)'
q 'sum by (instance) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic=""}[1m]))'
q 'kafka_server_raft_metrics_current_state_info{state="leader"}'
```

### B.4 Grafana

Ouvrez <http://localhost:3000> (utilisateur `admin`, mot de passe `kafka`) : le tableau de bord
**Kafka KRaft - Vue d'ensemble** s'affiche (dossier *Kafka*). Alertes Prometheus : <http://localhost:9090/alerts>.

---

## Partie C – Benchmark et optimisation

### C.1 Topic de test

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic perf-test --partitions 6 --replication-factor 3 \
  --config retention.ms=900000 --config segment.bytes=104857600
```

> Rétention de 15 min et segments de 100 Mo : les données de test sont supprimées rapidement.

### C.2 Producteur : mesure de référence puis optimisation

Lancez chaque test, gardez Grafana ouvert et notez les résultats (dernière ligne) dans le tableau.

```bash
# T1 - référence : pas de regroupement, pas de compression
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 200000 --record-size 1000 \
  --throughput -1 --command-property acks=all linger.ms=0 batch.size=16384 compression.type=none
# T2 - regroupement en lots plus gros
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 200000 --record-size 1000 \
  --throughput -1 --command-property acks=all linger.ms=20 batch.size=262144 compression.type=none
# T3 - lots + compression lz4
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 200000 --record-size 1000 \
  --throughput -1 --command-property acks=all linger.ms=20 batch.size=262144 compression.type=lz4
# T4 - comme T3 avec acks=1 (moins durable)
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 200000 --record-size 1000 \
  --throughput -1 --command-property acks=1 linger.ms=20 batch.size=262144 compression.type=lz4
```

```text
200000 records sent, 41322.3 records/sec (39.41 MB/sec), 612.27 ms avg latency, 1081.00 ms max latency, 590 ms 50th, 985 ms 95th, 1040 ms 99th, 1069 ms 99.9th.
```

| Test | `acks` | `linger.ms` | `batch.size` | compression | records/s | MB/s | latence p99 |
|---|---|---|---|---|---|---|---|
| T1 | all | 0 | 16 Ko | none | | | |
| T2 | all | 20 | 256 Ko | none | | | |
| T3 | all | 20 | 256 Ko | lz4 | | | |
| T4 | 1 | 20 | 256 Ko | lz4 | | | |

> Les valeurs absolues dépendent du poste (tous les brokers partagent le même disque) ; c'est la
> **tendance** qui compte : lots + compression = débit nettement supérieur, `acks=1` réduit la latence
> au prix de la durabilité. Option utile : `--print-metrics` (métriques détaillées du producteur).

### C.3 Consommateur

```bash
kafka-consumer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 400000 --group perf-consumer --timeout 60000
kafka-consumer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 400000 --group perf-consumer-2 --timeout 60000 \
  --command-property fetch.min.bytes=1048576 --command-property fetch.max.wait.ms=500 --command-property max.partition.fetch.bytes=4194304
```

> Attention : `kafka-consumer-perf-test.sh` (comme les outils *console*) attend **un** `--command-property`
> par propriété, alors que `kafka-producer-perf-test.sh` accepte plusieurs propriétés après un seul.

```text
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-11-09 11:02:10:120, 2026-11-09 11:02:16:455, 381.4697, 60.2140, 400000, 63141.2786, 3051, 3284, 116.1600, 121802.6797
```

### C.4 Leviers d'optimisation (tuning logiciel)

| Composant | Paramètre | Effet |
|---|---|---|
| Producer | `linger.ms`, `batch.size`, `compression.type` (lz4/zstd), `buffer.memory` | débit ↑ (latence légèrement ↑) |
| Producer | `acks`, `enable.idempotence`, `max.in.flight.requests.per.connection` | durabilité / ordre / latence |
| Consumer | `fetch.min.bytes`, `fetch.max.wait.ms`, `max.partition.fetch.bytes`, `max.poll.records` | débit ↑, moins de requêtes |
| Consumer | nombre d'instances ≤ nombre de partitions | parallélisme |
| Broker | `num.network.threads` (≈ cœurs/2), `num.io.threads` (≈ nb disques × 8) | si *AvgIdlePercent* < 0,3 |
| Broker | `num.replica.fetchers` | réplication plus rapide (ISR stables) |
| Broker | `socket.*.buffer.bytes`, `replica.socket.receive.buffer.bytes` | réseaux à forte latence |
| Broker | **ne pas** forcer `log.flush.*` | la durabilité vient de la réplication, pas du fsync |
| Topic | nombre de partitions | parallélisme ; trop de partitions = plus de mémoire, d'ouverture de fichiers, de réplication |

### C.5 Tuning système et JVM

| Domaine | Recommandation |
|---|---|
| OS | `vm.swappiness=1`, `vm.max_map_count=262144`, `nofile ≥ 100000`, `vm.dirty_*` (TP00 §5) |
| Disques | SSD/NVMe dédiés, XFS ou ext4 `noatime`, JBOD (plusieurs `log.dirs`) ou RAID 10 |
| Réseau | 10 Gb/s minimum en production ; même zone de latence pour le quorum |
| JVM | **Java 21** (ou 25), G1GC (défaut de Kafka : `-XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35`), heap **6 Go** pour un broker, 1 à 2 Go pour un contrôleur (prévoir ~5 Go de RAM pour les métadonnées des très gros clusters) |
| Mémoire | le reste de la RAM sert de **page cache** : viser au moins 30 s de débit d'écriture en cache |

Consommation actuelle des conteneurs :

```bash
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
docker exec broker-1 sh -c 'ls /opt/kafka/logs/ && tail -3 /opt/kafka/logs/kafkaServer-gc.log'
```

### C.6 Dimensionnement (méthode)

1. **Débit** : brokers ≥ (débit d'entrée × RF + débit de sortie) / débit soutenable par broker (disque **et** réseau, avec 30-40 % de marge).
2. **Stockage** : débit moyen × rétention × RF × 1,3 (marge). Exemple : 20 Mo/s × 3 jours × 3 × 1,3
   = 20 × 259 200 × 3 × 1,3 ≈ **20 To** au total, soit ~3,4 To par broker sur 6 brokers.
3. **Partitions** : `max(T / P, T / C)` avec T = débit cible, P (resp. C) = débit mesuré **par partition** pour
   un producteur (resp. consommateur) ; prévoir la croissance (on ne réduit jamais le nombre de partitions).
4. **Mémoire** : heap 6 Go + page cache ≥ débit d'écriture × 30 s par broker.
5. **Contrôleurs** : 3 (tolère 1 panne) ou 5 (tolère 2), sur des machines/zones distinctes.

---

## Partie D – Incidents et alertes

### D.1 Perte d'un broker

```bash
cd ~/KAFKA-KRAFT
docker compose stop broker-3
```

Observez pendant 2-3 minutes :

- Grafana : *Brokers actifs* = 2, *Sous-répliquées* > 0, *ISR shrink*
- Prometheus <http://localhost:9090/alerts> : `KafkaBrokerDown` puis `KafkaUnderReplicatedPartitions`
  passent de *Pending* à **Firing**

```bash
docker compose start broker-3
```

Les alertes repassent à l'état *Inactive* une fois l'ISR reconstitué.

### D.2 Lag d'un consumer group

Un groupe consomme 100 messages puis s'arrête ; on produit ensuite 50 000 messages :

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic perf-test --group lag-demo --max-messages 100 > /dev/null
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic perf-test --num-records 50000 --record-size 200 --throughput 2000
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group lag-demo | awk 'NR==1 || /perf-test/ {print $1, $2, $3, $6}'
```

- Grafana : panneau *Lag des consumer groups* (série `lag-demo / perf-test`)
- PromQL : `sum by (consumergroup, topic) (kafka_consumergroup_lag)`
- Après 2 min : alerte `KafkaConsumerGroupLagHigh`

Résorption du lag :

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic perf-test --group lag-demo --timeout-ms 20000 > /dev/null
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group lag-demo | awk 'NR==1 || /perf-test/ {print $1, $2, $3, $6}'
```

### D.3 (Optionnel) Métriques des clients : application Spring Boot

Lancez l'application du TP03 (`cd ~/KAFKA-KRAFT/java/spring-kafka-demo && mvn -q spring-boot:run`) :
le target `spring-kafka-demo` passe **UP** dans Prometheus. Requêtes utiles :

```text
kafka_consumer_fetch_manager_records_lag_max{client_id=~".*order-listener.*"}
rate(kafka_producer_record_send_total[1m])
kafka_producer_request_latency_avg
```

> L'application doit tourner **dans WSL** (terminal ou IntelliJ avec un JDK WSL) : Prometheus la joint
> via `host.docker.internal`.

---

## Partie E – ELISA (à évoquer)

**ELISA** est la plateforme de supervision de Kafka mise en oeuvre au **Crédit Agricole**. Le formateur
la présente rapidement à partir de l'annexe dédiée (`Annexe_ELISA.txt`) : périmètre, indicateurs suivis,
seuils d'alerte et mise en relation avec la chaîne Prometheus / Grafana de ce TP.

---

## Partie F – Fin du TP

```bash
cd ~/KAFKA-KRAFT
docker compose --profile monitoring stop      # ou laissez-le tourner si la mémoire le permet
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic perf-test
```

### Ce qu'il faut retenir

1. Les 4 indicateurs d'alerte immédiate : **contrôleur actif = 1**, **partitions hors ligne = 0**,
   **sous min ISR = 0**, **sous-répliquées = 0** (durablement).
2. Saturation : *RequestHandlerAvgIdlePercent* et *NetworkProcessorAvgIdlePercent* > 0,3.
3. Le **lag** se mesure à partir des offsets (kafka-exporter, Burrow, Kafka UI).
4. Débit producteur : `linger.ms` + `batch.size` + compression ; durabilité : `acks=all` + `min.insync.replicas=2`.
5. Dimensionner sur le débit, la rétention × RF, le parallélisme (partitions) et la mémoire (page cache).

### Questions de révision

1. Pourquoi `UnderReplicatedPartitions > 0` peut-il être normal pendant quelques secondes ?
2. Quelle métrique indique qu'il faut augmenter `num.io.threads` ?
3. Pourquoi ne faut-il pas donner toute la RAM du serveur à la heap du broker ?

✅ **Suite : TP06 – Sécuriser un cluster (synthèse).**
