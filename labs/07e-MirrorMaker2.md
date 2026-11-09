# TP07e – MirrorMaker 2 : réplication entre clusters et plan de reprise

> **Chapitre 7 du programme (écosystème)**
> Durée : 45 min
> Pré-requis : cluster du TP02, `source ~/KAFKA-KRAFT/scripts/kafka-env.sh`
> Nouveau TP (MirrorMaker 1 a été **supprimé** dans Kafka 4.0 : seul MirrorMaker 2 subsiste)

---

## Objectifs

- Comprendre l'architecture de **MirrorMaker 2** (MM2), bâti sur Kafka Connect
- Répliquer des topics d'un cluster **primary** (3 brokers) vers un cluster de secours **dr** (1 nœud KRaft)
- Vérifier la **traduction des offsets** des consumer groups et simuler une **bascule** des consommateurs
- Connaître les politiques de nommage et les topologies (actif/passif, actif/actif)

---

## Partie A – Rappels

```text
        cluster "primary" (broker-1..3)                                  cluster "dr" (kafka-dr)
  ┌──────────────────────────────────┐        MirrorMaker 2         ┌──────────────────────────────────┐
  │ mm2-demo, orders                 │ ──► MirrorSourceConnector ──►│ primary.mm2-demo, primary.orders │
  │ consumer group "mm2-group"       │ ──► MirrorCheckpointConnector►│ primary.checkpoints.internal     │
  │ mm2-offset-syncs.dr.internal     │     (traduction des offsets) │  + offsets du groupe "mm2-group" │
  │                                  │     MirrorHeartbeatConnector►│ heartbeats                       │
  └──────────────────────────────────┘                              └──────────────────────────────────┘
```

| Connecteur | Rôle |
|---|---|
| `MirrorSourceConnector` | copie les messages, crée les topics distants (même nombre de partitions), synchronise les configurations |
| `MirrorCheckpointConnector` | traduit les offsets des consumer groups (topic `<source>.checkpoints.internal`) et, si demandé, les applique aux groupes du cluster cible |
| `MirrorHeartbeatConnector` | émet des battements de cœur : supervision de la latence et de la topologie |

| Notion | Valeur par défaut |
|---|---|
| Nom des topics distants | `DefaultReplicationPolicy` : `<alias source>.<topic>` (évite les boucles en actif/actif) |
| Conserver les noms | `IdentityReplicationPolicy` (migration, actif/passif strict ; pas de détection de boucle) |
| Garantie | **au moins une fois** : en cas de bascule, quelques doublons possibles, pas de perte |
| Modes de déploiement | **dédié** (`connect-mirror-maker.sh`, utilisé ici) ou connecteurs sur un cluster **Kafka Connect** existant |

---

## Partie B – Configuration et démarrage

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/mm2
cat > config/mm2/mm2.properties <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  MirrorMaker 2 (mode dédié) : réplication du cluster "primary" vers "dr"
# =============================================================================
clusters = primary, dr
primary.bootstrap.servers = broker-1:19092,broker-2:19092,broker-3:19092
dr.bootstrap.servers = kafka-dr:19092

# Flux primary -> dr uniquement (actif / passif)
primary->dr.enabled = true
primary->dr.topics = mm2-demo.*, orders
primary->dr.groups = .*
dr->primary.enabled = false
# Sans cette ligne, MM2 crée quand même un "herder" dr->primary pour émettre des
# heartbeats DANS le cluster primary (topics internes mm2-*.dr.internal et heartbeats
# créés sur primary avec le facteur de réplication 1 ci-dessous, incompatible avec
# min.insync.replicas=2). En production, on le garde avec des facteurs de réplication de 3.
dr->primary.emit.heartbeats.enabled = false

# Synchronisation des offsets des consumer groups (bascule des consommateurs)
primary->dr.emit.checkpoints.enabled = true
primary->dr.emit.checkpoints.interval.seconds = 10
primary->dr.sync.group.offsets.enabled = true
primary->dr.sync.group.offsets.interval.seconds = 10
primary->dr.refresh.topics.interval.seconds = 10
primary->dr.refresh.groups.interval.seconds = 10
primary->dr.emit.heartbeats.enabled = true
# Synchronisation des configurations de topics (défaut : 600 s)
primary->dr.sync.topic.configs.interval.seconds = 10
# Démonstration : une synchronisation d'offset par message (défaut 100) pour une
# traduction exacte des offsets des groupes ; à laisser à la valeur par défaut en production
primary->dr.offset.lag.max = 0
# Fréquence de commit des workers Connect internes (défaut 60 s)
offset.flush.interval.ms = 10000
# Pas d'authorizer sur le cluster de formation : pas de synchronisation des ACL
sync.topic.acls.enabled = false

# Le cluster de secours n'a qu'un seul noeud : facteur de réplication 1 côté "dr"
replication.factor = 1
checkpoints.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
offset.storage.replication.factor = 1
status.storage.replication.factor = 1
config.storage.replication.factor = 1
# ... mais le topic mm2-offset-syncs est créé sur le cluster SOURCE (3 brokers,
# min.insync.replicas=2) : il lui faut donc un facteur de réplication de 3
offset-syncs.topic.replication.factor = 3

tasks.max = 2
EOF_KAFKA_KRAFT
```

Points de configuration importants :

- `primary->dr.topics` : liste d'expressions régulières (les nouveaux topics correspondants sont détectés toutes les 10 s) ;
- `replication.factor = 1` : le cluster de secours de la formation n'a qu'un nœud. **En production**, il a au moins 3 brokers et `replication.factor = 3` ;
- `offset-syncs.topic.replication.factor = 3` : le topic `mm2-offset-syncs.dr.internal` est créé sur le cluster **source** (`min.insync.replicas=2`) ;
- `min.insync.replicas` fait partie des configurations de topic **non** recopiées par défaut (`config.properties.exclude`).

Démarrage du cluster de secours et de MM2 (profil `mirror`) :

```bash
cd ~/KAFKA-KRAFT
docker compose --profile mirror up -d
docker compose ps kafka-dr mirror-maker
kafka-cluster.sh cluster-id --bootstrap-server $BOOTSTRAP_DR
```

```text
NAME           IMAGE                SERVICE        STATUS
kafka-dr       apache/kafka:4.4.0   kafka-dr       Up 40 seconds (healthy)
mirror-maker   apache/kafka:4.4.0   mirror-maker   Up 5 seconds
Cluster ID: Pkdd4qI5SaGzACER4PM-6A
```

Deux clusters indépendants : identifiants différents (`CLUSTER_ID` et `DR_CLUSTER_ID` du fichier `.env`).

---

## Partie C – Réplication des messages

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic mm2-demo --partitions 3 --replication-factor 3
seq 1 300 | sed 's/^/message-/' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic mm2-demo
sleep 20
kafka-topics.sh --bootstrap-server $BOOTSTRAP_DR --list
kafka-topics.sh --bootstrap-server $BOOTSTRAP_DR --describe --topic primary.mm2-demo
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic mm2-demo
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP_DR --topic primary.mm2-demo
```

```text
__consumer_offsets
heartbeats
mm2-configs.primary.internal
mm2-offsets.primary.internal
mm2-status.primary.internal
primary.checkpoints.internal
primary.mm2-demo
primary.orders
Topic: primary.mm2-demo	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs: ...
mm2-demo:0:104
mm2-demo:1:97
mm2-demo:2:99
primary.mm2-demo:0:104
primary.mm2-demo:1:97
primary.mm2-demo:2:99
```

> La répartition par partition varie ; elle est **identique** des deux côtés (même partition, même
> ordre). Les topics internes `mm2-*.primary.internal` sont ceux du worker Connect embarqué.

Lire la copie sur le cluster de secours :

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_DR --topic primary.mm2-demo --partition 0 --offset 0 --max-messages 3
```

Nouveau topic détecté automatiquement (expression régulière `mm2-demo.*`) :

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic mm2-demo-new --partitions 1 --replication-factor 3
echo "nouveau topic" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic mm2-demo-new
sleep 25
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_DR --topic primary.mm2-demo-new --from-beginning --max-messages 1
```

```text
nouveau topic
Processed a total of 1 messages
```

Synchronisation de configuration (toutes les 10 s dans notre configuration) :

```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --alter --entity-type topics --entity-name mm2-demo --add-config retention.ms=3600000
sleep 25
kafka-configs.sh --bootstrap-server $BOOTSTRAP_DR --describe --entity-type topics --entity-name primary.mm2-demo | grep -o 'retention.ms=[0-9]*'
```

```text
retention.ms=3600000
```

---

## Partie D – Traduction des offsets et bascule des consommateurs

### D.1 Le groupe consomme sur le cluster primary

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic mm2-demo --group mm2-group --from-beginning --timeout-ms 10000 > /dev/null
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group mm2-group
```

```text
Processed a total of 300 messages
Consumer group 'mm2-group' has no active members.
GROUP     TOPIC    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
mm2-group mm2-demo 0          104             104             0    -            -     -
...
```

Attendre la traduction des offsets (checkpoint toutes les 10 s, commit du worker toutes les 10 s) :

```bash
sleep 40
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_DR --list | grep mm2-group
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_DR --topic primary.checkpoints.internal --from-beginning --timeout-ms 5000 \
  --formatter org.apache.kafka.connect.mirror.formatters.CheckpointFormatter 2>/dev/null | tail -3
```

```text
mm2-group
Checkpoint{consumerGroupId=mm2-group, topicPartition=mm2-demo-0, upstreamOffset=104, downstreamOffset=104, metadata=}
...
```

### D.2 De nouveaux messages arrivent, puis le site primaire « tombe »

```bash
seq 301 350 | sed 's/^/message-/' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic mm2-demo
sleep 15
SUM='NR>1 && $2 ~ /mm2-demo$/ {c+=$4; e+=$5} END {print "current=" c, "log-end=" e, "lag=" e-c}'
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP    --describe --group mm2-group 2>/dev/null | awk "$SUM"
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP_DR --describe --group mm2-group 2>/dev/null | awk "$SUM"
```

```text
current=300 log-end=350 lag=50
current=300 log-end=350 lag=50
```

Le groupe `mm2-group` existe sur le cluster de secours avec des offsets **traduits** sur `primary.mm2-demo`.

### D.3 Bascule : le consommateur redémarre sur le cluster de secours

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_DR --topic primary.mm2-demo --group mm2-group --timeout-ms 10000 | sort -t- -k2 -n | sed -n '1p;$p'
```

```text
message-301
message-350
Processed a total of 50 messages
```

Le consommateur reprend **exactement** où il s'était arrêté : ni perte, ni relecture des 300 premiers
messages.

> En production (`offset.lag.max` = 100 par défaut), la traduction est **conservatrice** : le
> consommateur peut relire jusqu'à une centaine de messages par partition. Les traitements doivent
> donc être **idempotents**. MM2 ne traduit jamais un offset « en avant » (pas de perte).
> Les offsets ne sont appliqués au cluster cible que si le groupe y est **inactif**.

Application Java : il suffit de changer `bootstrap.servers` et de s'abonner au topic distant (ou à
l'expression régulière `(primary\.)?mm2-demo` pour fonctionner à l'identique sur les deux sites).

---

## Partie E – Topologies et exploitation

| Topologie | Configuration | Usage |
|---|---|---|
| Actif / passif | `primary->dr.enabled = true` | PRA, lecture analytique déportée |
| Actif / actif | `primary->dr` **et** `dr->primary` ; les préfixes empêchent les boucles | multi-sites, chaque site écrit localement |
| Agrégation | plusieurs sources → un cluster central | datalake, reporting |
| Migration | `IdentityReplicationPolicy`, flux temporaire | changement de cluster / montée de version majeure |

Supervision (JMX `kafka.connect.mirror:type=MirrorSourceConnector,target=dr,topic=*,partition=*`) :
`replication-latency-ms`, `record-age-ms`, `record-count`, `byte-rate` ; côté checkpoints :
`checkpoint-latency-ms`. Le lag du groupe interne de MM2 se lit avec kafka-exporter (TP05).

Bonnes pratiques :

- déployer MM2 **près du cluster cible** (consommer à distance, produire en local) ;
- ne **pas** répliquer `__consumer_offsets` ni les topics internes (filtres par défaut) ;
- surveiller la latence de réplication : c'est le **RPO** du plan de reprise ;
- tester la bascule **et** le retour (failback) régulièrement ;
- alternative commerciale sans traduction d'offsets : Confluent *Cluster Linking*.

---

## Partie F – Nettoyage

```bash
cd ~/KAFKA-KRAFT
docker compose --profile mirror stop mirror-maker kafka-dr
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic mm2-demo-new
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group mm2-group
```

> Le cluster `kafka-dr` garde ses données (volume `kafka-dr-data`) : `docker compose --profile mirror up -d`
> reprend la réplication là où elle s'était arrêtée.

### Ce qu'il faut retenir

1. MM2 = 3 connecteurs Kafka Connect (source, checkpoint, heartbeat) ; MM1 n'existe plus en 4.x.
2. Les topics distants sont préfixés par l'alias du cluster source (sauf `IdentityReplicationPolicy`).
3. La traduction d'offsets permet la bascule des consommateurs en **au moins une fois**.
4. La latence de réplication détermine le RPO ; la bascule doit être outillée et testée.

### Questions de révision

1. Pourquoi le topic distant s'appelle-t-il `primary.mm2-demo` et non `mm2-demo` ?
2. Sur quel cluster est créé le topic `mm2-offset-syncs.dr.internal` ? Pourquoi a-t-il besoin de RF=3 ici ?
3. Que se passe-t-il si le groupe `mm2-group` est actif sur le cluster dr pendant la synchronisation ?

✅ **Suite : TP08 – Conclusion.**
