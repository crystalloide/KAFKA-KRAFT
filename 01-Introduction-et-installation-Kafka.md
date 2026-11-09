# TP01 – Introduction à Kafka et installation du produit (mode KRaft)

> **Chapitre 1 du programme** – Objectifs pédagogiques n°1 et n°2
> Durée : 1 h 30 (dont 30 min d'apports théoriques)
> Pré-requis : TP00 réalisé (WSL2, Java 21, Docker, projet `~/KAFKA-KRAFT`)
> Reprend et met à jour : TP01, TP03, TP04, TP05, TP06 (Ubuntu / Kafka 4.1.1)

---

## Objectifs

- Comprendre les architectures orientées événements et le rôle de Kafka
- Installer **Apache Kafka 4.4.0** (distribution binaire) sous WSL2 selon les bonnes pratiques
- Démarrer un noeud **KRaft** en mode combiné (broker + contrôleur) et l'exploiter en service systemd
- Manipuler les concepts de base : **topic, partition, offset, producer, consumer, consumer group**
- Connaître les **variantes d'installation** (binaire, Docker, Kubernetes)

---

## Partie A – Rappels théoriques (synthèse)

### A.1 Architectures orientées événements et *event streaming*

| Approche | Principe | Limites |
|---|---|---|
| Appels synchrones (REST/RPC) | A appelle B et attend la réponse | Couplage temporel fort, effet domino des pannes |
| File de messages (JMS, AMQP) | A dépose un message, **un** consommateur le traite puis il est supprimé | Pas de relecture, peu adapté à N consommateurs indépendants |
| **Event streaming (Kafka)** | A publie un **événement** dans un **journal distribué, persistant et ordonné** ; N applications le lisent chacune à leur rythme | Demande de penser « flux » (ordre par clé, idempotence, schémas) |

Un **événement** = un fait qui s'est produit (« commande créée », « capteur à 21 °C ») : *clé*, *valeur*,
*horodatage*, *en-têtes*. Il est **immuable** et conservé selon une **politique de rétention**.

### A.2 Raison d'être de Kafka

- **Débit** : centaines de Mo/s par broker grâce aux écritures séquentielles, au *page cache* et au *zero-copy*
- **Durabilité** : journal répliqué sur plusieurs brokers (facteur de réplication, ISR)
- **Scalabilité horizontale** : partitions réparties sur les brokers, consommation parallèle
- **Découplage** : producteurs et consommateurs ignorent l'existence les uns des autres
- **Relecture** : les consommateurs gèrent leur position (**offset**), on peut « rembobiner »
- **Ecosystème** : Kafka Connect (intégration), Kafka Streams / ksqlDB (traitement), Schema Registry, MirrorMaker 2

### A.3 Principe de fonctionnement

```text
                     Topic "orders" (3 partitions, facteur de réplication 3)
 Producers          ┌─────────────────────────────────────────────────────────┐     Consumer group "facturation"
 ─────────►     P0  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ ...  ◄── leader sur broker 11   │ 	──► consumer A (P0, P1)
 (clé => P)     P1  │ 0 │ 1 │ 2 │ 3 │ ...          ◄── leader sur broker 12   │ 	──► consumer B (P2)
                P2  │ 0 │ 1 │ 2 │ 3 │ 4 │ ...      ◄── leader sur broker 13   │
                    └─────────────────────────────────────────────────────────┘     Consumer group "analytique"
                      offsets croissants, ajout en fin de journal (append-only)    	──► consumer C (P0, P1, P2)
```

- Un **topic** est découpé en **partitions** : unité de parallélisme et d'**ordre** (l'ordre n'est garanti
  qu'à l'intérieur d'une partition).
- Le **partitionneur** du producer choisit la partition : `hash(clé) % nb_partitions` si la clé est
  renseignée, sinon répartition « collante » (*sticky*) par lots.
- Chaque partition a un **leader** (lectures/écritures) et des **followers** qui répliquent ; l'**ISR**
  (*In-Sync Replicas*) liste les réplicas à jour.
- Un **consumer group** se partage les partitions : *une partition = un seul consommateur du groupe*.
  Deux groupes différents reçoivent chacun **tous** les messages (*publish/subscribe*).

### A.4 KRaft : Kafka sans ZooKeeper

Depuis **Kafka 4.0**, ZooKeeper a disparu : les métadonnées du cluster (topics, partitions, configs,
ACL...) sont stockées dans un **journal Raft** (`__cluster_metadata`) géré par un **quorum de
contrôleurs**.

| Rôle (`process.roles`) | Usage |
|---|---|
| `broker` | Stocke les données, sert les clients |
| `controller` | Membre du quorum Raft des métadonnées ; un contrôleur **actif** (leader Raft) |
| `broker,controller` | Mode **combiné** : développement / tests uniquement |

Un quorum de **3** contrôleurs tolère **1** panne, **5** contrôleurs en tolèrent **2** (majorité).

### A.5 Variantes d'installation

| Variante | Usage | Remarques |
|---|---|---|
| Archive binaire `kafka_2.13-4.4.0.tgz` | VM / serveurs physiques | systemd, Ansible ; c'est ce que nous faisons dans ce TP |
| Image Docker `apache/kafka` (JVM) | Développement, CI, ce cours | Configuration par variables `KAFKA_*` |
| Image Docker `apache/kafka-native` (GraalVM) | Tests rapides | Démarrage en ~1 s, **non supportée en production** |
| Kubernetes : opérateur **Strimzi** (CNCF), Confluent for Kubernetes | Production conteneurisée | CRD `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser` |
| Services managés (Confluent Cloud, Amazon MSK, Aiven...) | Pas d'exploitation | Coût, souveraineté |

Pré-requis : **Java 17+** pour les brokers (Java 21 ou 25 recommandés), Java 11+ pour les clients.

---

## Partie B – Installation de Kafka 4.4.0 (distribution binaire)

### B.1 Télécharger et vérifier l'archive

```bash
cd /tmp
KAFKA_VERSION=4.4.0
SCALA_VERSION=2.13
ARCHIVE=kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz
for base in https://downloads.apache.org/kafka https://archive.apache.org/dist/kafka; do
  curl -fSLO "${base}/${KAFKA_VERSION}/${ARCHIVE}" && curl -fSLO "${base}/${KAFKA_VERSION}/${ARCHIVE}.sha512" && break
done
ls -lh ${ARCHIVE}*
```

Vérification de l'empreinte SHA-512 (le fichier `.sha512` d'Apache est au format GPG) :

```bash
EXPECTED=$(sed 's/^[^:]*://' ${ARCHIVE}.sha512 | tr -d ' \n\r' | tr 'A-F' 'a-f')
ACTUAL=$(sha512sum ${ARCHIVE} | cut -d' ' -f1)
[ "$EXPECTED" = "$ACTUAL" ] && echo "Empreinte OK" || echo "ERREUR : empreinte invalide"
```

Affichage attendu : `Empreinte OK`

### B.2 Installer dans /opt (convention production)

```bash
sudo tar -xzf /tmp/${ARCHIVE} -C /opt
sudo ln -sfn /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka
sudo chown -R "$USER":"$USER" /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}
ls -l /opt | grep kafka
```

```text
lrwxrwxrwx  1 root root   22 ... kafka -> /opt/kafka_2.13-4.4.0
drwxr-xr-x  7 user user 4096 ... kafka_2.13-4.4.0
```

> Le lien symbolique `/opt/kafka` permet une **montée de version** sans modifier les scripts :
> on installe la nouvelle version à côté puis on bascule le lien.

Ajouter les outils au `PATH` pour tous les utilisateurs :

```bash
printf 'export KAFKA_HOME=/opt/kafka\nexport PATH=$PATH:$KAFKA_HOME/bin\n' | sudo tee /etc/profile.d/kafka.sh
source /etc/profile.d/kafka.sh
kafka-topics.sh --version
```

Affichage attendu : `4.4.0`

### B.3 Découvrir la distribution

```bash
ls /opt/kafka
```
```bash
ls /opt/kafka/bin | head -50
```
```bash
ls /opt/kafka/config
```

```text
LICENSE  NOTICE  bin  config  libs  licenses  site-docs
...
broker.properties  connect-distributed.properties  connect-standalone.properties  controller.properties
log4j2.yaml  producer.properties  consumer.properties  server.properties  tools-log4j2.yaml ...
```

- `bin/` : scripts (`kafka-server-start.sh`, `kafka-topics.sh`, `kafka-console-producer.sh`...) ;
  sous Windows natif : `bin/windows/*.bat` (non utilisé ici)
- `config/server.properties` : mode combiné ; `broker.properties` / `controller.properties` : rôles séparés
- Plus de répertoire `config/kraft/` ni de `zookeeper.properties` depuis Kafka 4.0

---

## Partie C – Premier noeud KRaft (mode combiné)

### C.1 Fichier de configuration du noeud

Nous utilisons les ports **9192** (clients) et **9193** (contrôleur) pour ne pas entrer en conflit avec
le cluster Docker du TP02 (ports 9092/9094/9096).

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/standalone
cat > config/standalone/server.properties <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  TP01 - Noeud Kafka 4.4.0 "standalone" (mode combiné broker + contrôleur)
#  Installation binaire sous WSL2 : /opt/kafka/config/standalone/server.properties
#  Ports 9192 (clients) et 9193 (contrôleur) : pas de conflit avec le cluster Docker
# =============================================================================

############################# Rôles et identité #############################
# Mode combiné : réservé au développement (en production : rôles séparés)
process.roles=broker,controller
node.id=1
# Quorum "dynamique" (KIP-853) : formaté avec "kafka-storage.sh format --standalone"
controller.quorum.bootstrap.servers=localhost:9193

############################# Réseau #############################
listeners=PLAINTEXT://:9192,CONTROLLER://:9193
advertised.listeners=PLAINTEXT://localhost:9192,CONTROLLER://localhost:9193
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

############################# Stockage #############################
log.dirs=/var/lib/kafka-standalone/data
num.partitions=3
num.recovery.threads.per.data.dir=2
# Un seul noeud : facteur de réplication 1 pour les topics internes
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
# Les topics doivent être créés explicitement (recommandé en production)
auto.create.topics.enable=false

############################# Rétention #############################
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

############################# Groupes #############################
# 0 en développement (3000 par défaut, recommandé en production)
group.initial.rebalance.delay.ms=0
EOF_KAFKA_KRAFT
```

Installation du fichier et des répertoires de données et de logs :

```bash
mkdir -p /opt/kafka/config/standalone
cp ~/KAFKA-KRAFT/config/standalone/server.properties /opt/kafka/config/standalone/
sudo mkdir -p /var/lib/kafka-standalone/data /var/log/kafka-standalone
sudo chown -R "$USER":"$USER" /var/lib/kafka-standalone /var/log/kafka-standalone
```

### C.2 Générer l'identifiant du cluster et formater le stockage

En mode KRaft, **avant le premier démarrage**, chaque noeud doit formater son répertoire avec le
**même `cluster.id`** :

```bash
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
echo "$KAFKA_CLUSTER_ID" | tee ~/kafka-standalone-cluster-id.txt
kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c /opt/kafka/config/standalone/server.properties
```

Affichage attendu :

```text
Bootstrap metadata: BootstrapMetadata(records=[...], metadataVersionLevel=..., source=format command)
Formatting dynamic metadata voter directory /var/lib/kafka-standalone/data with metadata.version 4.4-IV2.
```

> `--standalone` initialise un **quorum dynamique** (KIP-853) d'un seul contrôleur, qui pourra être
> étendu à chaud avec `kafka-metadata-quorum.sh add-controller`. Le cluster Docker du TP02 utilise au
> contraire un quorum **statique** (`controller.quorum.voters`).

Relancer la commande montre la protection contre un reformatage accidentel :

```bash
kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c /opt/kafka/config/standalone/server.properties
```

```text
Log directory /var/lib/kafka-standalone/data is already formatted. Use --ignore-formatted to ignore this directory and format the others.
```

Contenu de l'en-tête de stockage :

```bash
cat /var/lib/kafka-standalone/data/meta.properties
```

```text
#
#... (date)
node.id=1
directory.id=xxxxxxxxxxxxxxxxxxxxxx
version=1
cluster.id=<votre KAFKA_CLUSTER_ID>
```

### C.3 Démarrage au premier plan

```bash
kafka-server-start.sh /opt/kafka/config/standalone/server.properties
```

Ligne attendue en fin de démarrage :

```text
[... ] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
```

Dans un **second terminal** :

```bash
ss -tlnp | grep -E ':919[23]'
kafka-metadata-quorum.sh --bootstrap-server localhost:9192 describe --status
kafka-features.sh --bootstrap-server localhost:9192 describe
```

Affichage attendu (extraits) :

```text
LISTEN 0  50  *:9192  *:*  users:(("java",pid=...))
LISTEN 0  50  *:9193  *:*  users:(("java",pid=...))

ClusterId:              <votre KAFKA_CLUSTER_ID>
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          ...
MaxFollowerLag:         0
CurrentVoters:          [{"id": 1, "directoryId": "...", "endpoints": ["CONTROLLER://localhost:9193"]}]
CurrentObservers:       []

Feature: eligible.leader.replicas.version  SupportedMinVersion: 0  SupportedMaxVersion: 1  FinalizedVersionLevel: 1 ...
Feature: group.version                      ...  FinalizedVersionLevel: 1 ...
Feature: kraft.version                      ...  FinalizedVersionLevel: 1 ...
Feature: metadata.version                   ...  FinalizedVersionLevel: 4.4-IV2 ...
Feature: share.version                      ...
Feature: transaction.version                ...
```

Arrêtez le serveur avec **Ctrl+C** dans le premier terminal (arrêt contrôlé : `shutting down`, `shut down completed`).

> ⚠️ **N'utilisez pas `kafka-server-stop.sh` dans cet environnement** : ce script envoie un signal à
> **tous** les processus `kafka.Kafka` visibles... y compris ceux des **conteneurs Docker** du TP02
> (ils tournent sur le même noyau et avec le même UID 1000). Utilisez Ctrl+C ou `systemctl stop`.

### C.4 Exploitation en service systemd (bonne pratique production)

```bash
cd ~/KAFKA-KRAFT
mkdir -p system
cat > system/kafka-standalone.service <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Service systemd du noeud Kafka standalone (TP01)
#  Installation : sed "s/@USER@/$USER/g" system/kafka-standalone.service \
#                   | sudo tee /etc/systemd/system/kafka-standalone.service
# =============================================================================
[Unit]
Description=Apache Kafka 4.4.0 - noeud standalone KRaft (formation)
Documentation=https://kafka.apache.org/documentation/
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=@USER@
Group=@USER@
Environment="KAFKA_HEAP_OPTS=-Xms512m -Xmx512m"
Environment="LOG_DIR=/var/log/kafka-standalone"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/standalone/server.properties
# SIGTERM déclenche l'arrêt contrôlé de Kafka ; 143 = arrêt normal sur SIGTERM
KillSignal=SIGTERM
TimeoutStopSec=120
SuccessExitStatus=143
Restart=on-failure
RestartSec=10
# Réglages "production" : descripteurs de fichiers et processus
LimitNOFILE=100000
LimitNPROC=65536

[Install]
WantedBy=multi-user.target
EOF_KAFKA_KRAFT
```

```bash
sed "s/@USER@/$USER/g" ~/KAFKA-KRAFT/system/kafka-standalone.service \
  | sudo tee /etc/systemd/system/kafka-standalone.service > /dev/null
sudo systemctl daemon-reload
sudo systemctl start kafka-standalone
sleep 10
systemctl status kafka-standalone --no-pager | head -5
```

```text
● kafka-standalone.service - Apache Kafka 4.4.0 - noeud standalone KRaft (formation)
     Loaded: loaded (/etc/systemd/system/kafka-standalone.service; disabled; preset: enabled)
     Active: active (running) since ...
```

Logs du service : `journalctl -u kafka-standalone -f` (Ctrl+C pour quitter) et fichiers dans
`/var/log/kafka-standalone/` (`server.log`, `controller.log`, `kafkaServer-gc.log`...).

> Pour un démarrage automatique au boot : `sudo systemctl enable kafka-standalone`. Nous ne le faisons
> pas ici pour économiser la mémoire de la VM.

---

## Partie D – Concepts de base en pratique

Variable pratique pour la suite de ce TP :

```bash
export BS=localhost:9192
```

### D.1 Topics

```bash
kafka-topics.sh --bootstrap-server $BS --create --topic premier-topic --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server $BS --list
kafka-topics.sh --bootstrap-server $BS --describe --topic premier-topic
```

```text
Created topic premier-topic.
premier-topic
Topic: premier-topic	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1,segment.bytes=1073741824
	Topic: premier-topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: premier-topic	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: premier-topic	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
```

> `auto.create.topics.enable=false` : écrire dans un topic inexistant échoue. C'est le réglage
> recommandé en production (pas de topic créé par une faute de frappe, avec de mauvais paramètres).

### D.2 Publier et consommer (publish / subscribe)

Terminal 1 – un **consumer** qui lit depuis le début :

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic premier-topic --from-beginning
```

Terminal 2 – un **producer** (une ligne = un message, Ctrl+D pour quitter) :

```bash
kafka-console-producer.sh --bootstrap-server localhost:9192 --topic premier-topic
```

```text
>Bonjour Kafka
>Deuxième message
>Troisième message
```

Les messages apparaissent dans le terminal 1. Quittez le consumer (Ctrl+C) puis relancez-le **sans**
`--from-beginning` : il n'affiche plus rien (position par défaut = fin du journal, `latest`).

### D.3 Messages avec clé : partitionnement et ordre

```bash
kafka-console-producer.sh --bootstrap-server localhost:9192 --topic premier-topic \
  --reader-property parse.key=true --reader-property key.separator=:
```

```text
>client-1:commande A
>client-2:commande B
>client-1:commande C
>client-3:commande D
>client-1:commande E
```

Lecture avec affichage de la partition, de l'offset, de l'horodatage et de la clé :

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic premier-topic --from-beginning \
  --formatter-property print.timestamp=true --formatter-property print.partition=true \
  --formatter-property print.offset=true --formatter-property print.key=true --timeout-ms 5000
```

Affichage attendu (les numéros de partition peuvent différer) :

```text
CreateTime:1762700000000	Partition:2	Offset:0	null	Bonjour Kafka
...
CreateTime:1762700100000	Partition:0	Offset:0	client-1	commande A
CreateTime:1762700100100	Partition:0	Offset:1	client-1	commande C
CreateTime:1762700100200	Partition:0	Offset:2	client-1	commande E
...
```

> **Toutes les commandes de `client-1` sont dans la même partition, dans l'ordre d'envoi** : c'est la
> garantie d'ordre de Kafka (par clé / par partition). L'ordre **global** entre partitions n'existe pas.

> Kafka 4.2+ : `--property` (déprécié) est remplacé par `--reader-property` (producer) et
> `--formatter-property` (consumer) ; `--producer-property`/`--consumer-property` par `--command-property`.

### D.4 Lire une partition à partir d'un offset donné

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic premier-topic \
  --partition 0 --offset 1 --formatter-property print.offset=true --timeout-ms 5000
```

> `--offset` exige `--partition` et ne se combine pas avec `--from-beginning` (utiliser `--offset earliest`).

### D.5 Consumer groups

Terminal 1 et Terminal 3 : deux consumers dans le **même** groupe `facturation` :

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic premier-topic --group facturation \
  --formatter-property print.partition=true
```

Terminal 4 : un consumer d'un **autre** groupe `analytique` :

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic premier-topic --group analytique \
  --formatter-property print.partition=true
```

Terminal 2 : envoyez une dizaine de messages avec des clés différentes (`a:1`, `b:2`, `c:3`, ...).
Observez : les deux consumers de `facturation` **se répartissent** les partitions, le consumer
`analytique` reçoit **tout**. Puis, dans un autre terminal :

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9192 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9192 --describe --group facturation
```

```text
GROUP        TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID              HOST        CLIENT-ID
facturation  premier-topic  0          5               5               0    console-consumer-1a2b... /127.0.0.1  console-consumer
facturation  premier-topic  1          4               4               0    console-consumer-1a2b... /127.0.0.1  console-consumer
facturation  premier-topic  2          6               6               0    console-consumer-9f8e... /127.0.0.1  console-consumer
```

- `CURRENT-OFFSET` : prochain offset que le groupe lira (offset **commité**)
- `LOG-END-OFFSET` : offset du prochain message qui sera écrit
- `LAG` = LOG-END-OFFSET − CURRENT-OFFSET : messages en retard

Arrêtez tous les consumers (Ctrl+C).

### D.6 Où sont les données ? Le journal sur disque

```bash
ls /var/lib/kafka-standalone/data/
ls -l /var/lib/kafka-standalone/data/premier-topic-0/
kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka-standalone/data/premier-topic-0/00000000000000000000.log | head -20
```

```text
__cluster_metadata-0  __consumer_offsets-0 ... premier-topic-0  premier-topic-1  premier-topic-2  meta.properties ...
00000000000000000000.index  00000000000000000000.log  00000000000000000000.timeindex  leader-epoch-checkpoint  partition.metadata
Dumping /var/lib/kafka-standalone/data/premier-topic-0/00000000000000000000.log
Log starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 ... CreateTime: ... size: ... compresscodec: none ...
| offset: 0 CreateTime: ... keySize: 8 valueSize: 10 ... key: client-1 payload: commande A
```

- Une partition = un répertoire ; des **segments** (`.log` + index `.index` et `.timeindex`)
- `__consumer_offsets` : topic interne où sont stockés les offsets commités des groupes
- `__cluster_metadata` : journal Raft des métadonnées (KRaft)

---

## Partie E – Variante : Kafka dans un conteneur Docker

```bash
docker run -d --rm --name kafka-test -p 9092:9092 apache/kafka:4.4.0
sleep 15
docker logs kafka-test | grep -m1 "Kafka Server started"
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-docker --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec -e KAFKA_OPTS= kafka-test /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker stop kafka-test
```

```text
[...] INFO [KafkaRaftServer nodeId=1] Kafka Server started (kafka.server.KafkaRaftServer)
Created topic test-docker.
test-docker
test-docker
```

> L'image officielle se configure par variables d'environnement : `KAFKA_NODE_ID` → `node.id`,
> `KAFKA_PROCESS_ROLES` → `process.roles`, etc. (`_` → `.`, `__` → `_`, `___` → `-`).
> **Dès qu'une variable `KAFKA_*` est fournie, la configuration par défaut de l'image est ignorée** :
> toute la configuration doit alors être décrite. C'est ce que fait le `docker-compose.yml` du TP02.
> Variante GraalVM (démarrage quasi instantané, tests uniquement) : `apache/kafka-native:4.4.0`.

---

## Partie F – Fin du TP

Le noeud standalone ne sera plus utilisé (sauf en TP07a, Kafka Connect *standalone*). Arrêtez-le pour
libérer la mémoire :

```bash
sudo systemctl stop kafka-standalone
systemctl is-active kafka-standalone
```

### Ce qu'il faut retenir

1. Kafka 4.x fonctionne **uniquement en KRaft** : `cluster.id` + `kafka-storage.sh format` avant le 1er démarrage.
2. Un topic = N partitions ; l'**ordre** est garanti **par partition** ; la **clé** détermine la partition.
3. Un consumer group se **partage** les partitions ; deux groupes lisent **indépendamment** (pub/sub).
4. Les offsets commités sont stockés dans `__consumer_offsets` ; le **lag** mesure le retard.
5. Production : service systemd, utilisateur dédié, `LimitNOFILE`, répertoires de données dédiés,
   **jamais** le mode combiné.

### Questions de révision

1. Pourquoi l'ordre global d'un topic à 3 partitions n'est-il pas garanti ?
2. Que se passe-t-il si un 4e consumer rejoint un groupe qui lit un topic à 3 partitions ?
3. Quelle commande permet de connaître le contrôleur actif d'un cluster KRaft ?
4. Pourquoi faut-il conserver le `cluster.id` ?

✅ **Suite : TP02 – Installer et configurer un cluster KRaft.**
