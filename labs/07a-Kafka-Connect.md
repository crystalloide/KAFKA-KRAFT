# TP07a – Kafka Connect : intégration de données

> **Chapitre 7 du programme (écosystème)** – Objectif pédagogique n°5
> Durée : 1 h 30
> Pré-requis : cluster du TP02, outils CLI (TP01), `source ~/KAFKA-KRAFT/scripts/kafka-env.sh`
> Reprend et met à jour : TP08 (stand-alone), TP09 (JSON et schémas), TP10 (SMT), TP11 (chaînage des SMT),
> TP28 (génération de données) ; ajout du mode **distribué**, de la **Dead Letter Queue** et d'un pipeline
> **PostgreSQL → Kafka → PostgreSQL** (CDC Debezium)

---

## Objectifs

- Comprendre l'architecture de Kafka Connect : **workers, connecteurs, tâches, convertisseurs, SMT**
- Utiliser le mode **standalone** puis le mode **distribué** (API REST)
- Maîtriser les **convertisseurs** (String, JSON avec/sans schéma) et les **transformations** (SMT)
- Gérer les **erreurs** (tolérance, journalisation, Dead Letter Queue)
- Construire un pipeline d'**intégration de données** : capture de changements (CDC) d'une base
  PostgreSQL et réplication vers une table cible

---

## Partie A – Rappels

```text
 Système source            Kafka Connect (cluster de workers)                       Système cible
 ─────────────            ─────────────────────────────────────────────────         ─────────────
 fichier, BDD,   ──►  [Source connector] ─► tâches ─► SMT ─► Converter ─► topic ─► Converter ─► SMT ─► [Sink connector] ─► BDD, fichier,
 SaaS, MQ...            (lit la source)                         (sérialise)        (désérialise)       (écrit la cible)    S3, Elastic...
                        offsets source stockés dans _connect-offsets ; configs dans _connect-configs ; état dans _connect-status
```

| Notion | Rôle |
|---|---|
| Worker | processus JVM Connect ; en mode **distribué**, les workers d'un même `group.id` forment un cluster |
| Connecteur | plugin + configuration (JSON) ; découpe le travail en **tâches** (`tasks.max`) |
| Converter | format dans Kafka : `StringConverter`, `JsonConverter` (`schemas.enable`), Avro/Protobuf/JSON Schema (Schema Registry) |
| SMT | transformation simple d'un message à la volée (masquer, renommer, insérer, router...) |
| Plugins | répertoire `plugin.path` (isolation des classes par plugin) |

Mode **standalone** : un seul processus, configuration par fichiers, offsets dans un fichier local
(tests). Mode **distribué** : tolérance aux pannes, scalabilité, API REST, état dans Kafka (production).

---

## Partie B – Mode standalone (ex-TP08) avec la distribution binaire

Le worker *standalone* s'exécute sur l'hôte avec `/opt/kafka/bin/connect-standalone.sh` et se connecte au
cluster Docker. Les connecteurs **FileStream** sont fournis avec Kafka (`libs/connect-file-4.4.0.jar`) mais
ne sont plus chargés par défaut : il faut les déclarer dans `plugin.path`.

```bash
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
mkdir -p ~/connect-standalone && cd ~/connect-standalone
cat > worker.properties <<EOF
bootstrap.servers=${BOOTSTRAP}
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
offset.storage.file.filename=$HOME/connect-standalone/standalone.offsets
offset.flush.interval.ms=5000
# 8083 est utilisé par le worker distribué (Partie C)
listeners=http://localhost:8084
plugin.path=/opt/kafka/libs/connect-file-4.4.0.jar
EOF
cat > file-source.properties <<EOF
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file=$HOME/connect-standalone/source.txt
topic=connect-standalone
EOF
cat > file-sink.properties <<EOF
name=local-file-sink
connector.class=FileStreamSink
tasks.max=1
file=$HOME/connect-standalone/sink.txt
topics=connect-standalone
EOF
touch source.txt
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic connect-standalone --partitions 1 --replication-factor 3
```

> ⚠️ `topic` (source) mais `topics` (sink, liste) : erreur fréquente.

Démarrage du worker avec les deux connecteurs (terminal dédié) :

```bash
cd ~/connect-standalone
connect-standalone.sh worker.properties file-source.properties file-sink.properties
```

```text
... INFO [local-file-source|task-0] ... WorkerSourceTask{id=local-file-source-0} Source task finished initialization and start ...
... INFO [local-file-sink|task-0] ... WorkerSinkTask{id=local-file-sink-0} Sink task finished initialization and start ...
```

Dans un autre terminal, ajoutez des lignes au fichier source (avec `echo >>` : chaque ligne doit se
terminer par un retour à la ligne pour être prise en compte) :

```bash
cd ~/connect-standalone
echo "Test de message depuis Kafka Connect source" >> source.txt
echo "une ligne de plus" >> source.txt
sleep 3
cat sink.txt
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic connect-standalone --from-beginning --timeout-ms 5000
curl -s localhost:8084/connectors | jq
```

```text
Test de message depuis Kafka Connect source
une ligne de plus
...
["local-file-source","local-file-sink"]
```

Arrêtez le worker standalone (Ctrl+C) : la suite utilise le mode distribué.

---

## Partie C – Mode distribué (worker Docker) et API REST

### C.1 Configuration du worker

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/connect
cat > config/connect/connect-distributed.properties <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  Worker Kafka Connect en mode DISTRIBUE (conteneur "connect")
# =============================================================================
bootstrap.servers=broker-1:19092,broker-2:19092,broker-3:19092
group.id=connect-cluster

# Convertisseurs par défaut (surchargeables connecteur par connecteur)
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true

# Topics internes (créés automatiquement par le worker, RF=3 comme en production)
config.storage.topic=_connect-configs
config.storage.replication.factor=3
offset.storage.topic=_connect-offsets
offset.storage.replication.factor=3
offset.storage.partitions=25
status.storage.topic=_connect-status
status.storage.replication.factor=3
status.storage.partitions=5
offset.flush.interval.ms=10000

# API REST
listeners=http://0.0.0.0:8083
rest.advertised.host.name=connect
rest.advertised.port=8083

# Plugins : connecteurs FileStream fournis avec Kafka + plugins téléchargés (Debezium)
plugin.path=/opt/kafka/libs/connect-file-4.4.0.jar,/opt/connect-plugins
plugin.discovery=hybrid_warn

# Autorise les surcharges producer.override.* / consumer.override.* par connecteur
connector.client.config.override.policy=All
EOF_KAFKA_KRAFT
```

Données d'entrée du connecteur source (copiées dans `connect/data/input/` par `scripts/setup.sh`) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p data
cat > data/films.txt <<'EOF_KAFKA_KRAFT'
1986;Top Gun
2022;Top Gun Maverick
1977;La Guerre des etoiles
1999;Matrix
2010;Inception
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/data/input
cat > connect/data/input/films.txt <<'EOF_KAFKA_KRAFT'
1986;Top Gun
2022;Top Gun Maverick
1977;La Guerre des etoiles
1999;Matrix
2010;Inception
EOF_KAFKA_KRAFT
```

Démarrage du profil `connect` (worker + PostgreSQL utilisé en Partie F) :

```bash
cd ~/KAFKA-KRAFT
./scripts/setup.sh > /dev/null
docker compose --profile connect up -d
sleep 45
docker compose --profile connect ps connect postgres --format "table {{.Name}}\t{{.Status}}"
```

```text
NAME       STATUS
connect    Up About a minute (healthy)
postgres   Up About a minute (healthy)
```

### C.2 Explorer l'API REST

```bash
curl -s localhost:8083/ | jq
curl -s localhost:8083/connector-plugins | jq -r '.[] | "\(.type)\t\(.class)\t\(.version)"'
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep _connect
```

```text
{ "version": "4.4.0", "commit": "...", "kafka_cluster_id": "PIYzDzl8TomQ84Cnrxi0gA" }
sink	org.apache.kafka.connect.file.FileStreamSinkConnector	4.4.0
source	org.apache.kafka.connect.file.FileStreamSourceConnector	4.4.0
source	org.apache.kafka.connect.mirror.MirrorCheckpointConnector	4.4.0
source	org.apache.kafka.connect.mirror.MirrorHeartbeatConnector	4.4.0
source	org.apache.kafka.connect.mirror.MirrorSourceConnector	4.4.0
_connect-configs
_connect-offsets
_connect-status
```

| Méthode | Ressource | Rôle |
|---|---|---|
| `GET` | `/connectors?expand=status` | liste et état |
| `PUT` | `/connectors/{nom}/config` | **crée ou met à jour** (idempotent) |
| `GET` | `/connectors/{nom}/status` | état du connecteur et des tâches |
| `PUT` | `/connectors/{nom}/pause` / `resume` / `stop` | cycle de vie |
| `POST` | `/connectors/{nom}/restart?includeTasks=true&onlyFailed=true` | redémarrage |
| `GET` / `PATCH` / `DELETE` | `/connectors/{nom}/offsets` | lire / modifier / réinitialiser les offsets (connecteur stoppé) |
| `DELETE` | `/connectors/{nom}` | suppression |
| `PUT` | `/connector-plugins/{classe}/config/validate` | validation d'une configuration |

### C.3 Connecteurs FileStream source et sink

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/connectors
cat > connect/connectors/file-source-films.json <<'EOF_KAFKA_KRAFT'
{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
  "tasks.max": "1",
  "file": "/data/input/films.txt",
  "topic": "films",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/connectors
cat > connect/connectors/file-sink-films.json <<'EOF_KAFKA_KRAFT'
{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
  "tasks.max": "1",
  "topics": "films",
  "file": "/data/output/films-sink.txt",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter"
}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/films-source/config \
  --data @connect/connectors/file-source-films.json | jq '.name'
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/films-sink/config \
  --data @connect/connectors/file-sink-films.json | jq '.name'
sleep 5
curl -s "localhost:8083/connectors?expand=status" | jq -r '.[] | "\(.status.name)\t\(.status.connector.state)\t\(.status.tasks[0].state)"'
cat connect/data/output/films-sink.txt
```

```text
"films-source"
"films-sink"
films-sink	RUNNING	RUNNING
films-source	RUNNING	RUNNING
1986;Top Gun
2022;Top Gun Maverick
1977;La Guerre des etoiles
1999;Matrix
2010;Inception
```

Ajout d'une ligne au fichier source (le répertoire est monté dans le conteneur) :

```bash
echo "2021;Dune" >> connect/data/input/films.txt
sleep 5
tail -1 connect/data/output/films-sink.txt
curl -s localhost:8083/connectors/films-source/offsets | jq
```

```text
2021;Dune
{ "offsets": [ { "partition": { "filename": "/data/input/films.txt" }, "offset": { "position": 112 } } ] }
```

### C.4 Convertisseur JSON : avec et sans schéma (ex-TP09)

Avec `JsonConverter` et `schemas.enable=true` (valeur par défaut du worker), chaque message embarque son
schéma. Mettez à jour le connecteur source pour utiliser le convertisseur du worker :

```bash
jq 'del(."value.converter")' connect/connectors/file-source-films.json \
  | jq '. + {"topic": "films-json-envelope"}' > /tmp/films-source-json.json
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic films-json-envelope --partitions 1 --replication-factor 3
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/films-source-json/config --data @/tmp/films-source-json.json | jq -r .name
sleep 5
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic films-json-envelope --from-beginning --max-messages 1
curl -s -X DELETE localhost:8083/connectors/films-source-json
```

```text
{"schema":{"type":"string","optional":false},"payload":"1986;Top Gun"}
```

Inversement, un **sink** configuré en JSON avec schéma attend l'enveloppe `{"schema":...,"payload":...}`.

### C.5 Transformations SMT et leur chaînage (ex-TP10 / TP11)

Messages structurés (schéma + données) envoyés dans le topic `films-json` :

```bash
cd ~/KAFKA-KRAFT
mkdir -p data
cat > data/films-json.txt <<'EOF_KAFKA_KRAFT'
{"schema":{"type":"struct","name":"film","optional":false,"fields":[{"field":"DateFilm","type":"string","optional":false},{"field":"TitreFilm","type":"string","optional":false}]},"payload":{"DateFilm":"2022","TitreFilm":"Top Gun Maverick"}}
{"schema":{"type":"struct","name":"film","optional":false,"fields":[{"field":"DateFilm","type":"string","optional":false},{"field":"TitreFilm","type":"string","optional":false}]},"payload":{"DateFilm":"1999","TitreFilm":"Matrix"}}
{"schema":{"type":"struct","name":"film","optional":false,"fields":[{"field":"DateFilm","type":"string","optional":false},{"field":"TitreFilm","type":"string","optional":false}]},"payload":{"DateFilm":"2010","TitreFilm":"Inception"}}
EOF_KAFKA_KRAFT
```

Connecteur sink avec **3 SMT chaînées** (ordre d'application = ordre de la liste `transforms`) et une
**Dead Letter Queue** :

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/connectors
cat > connect/connectors/file-sink-films-smt.json <<'EOF_KAFKA_KRAFT'
{
  "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
  "tasks.max": "1",
  "topics": "films-json",
  "file": "/data/output/films-smt.txt",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true",
  "transforms": "masquerTitre,ajouterSource,renommer",
  "transforms.masquerTitre.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.masquerTitre.fields": "TitreFilm",
  "transforms.masquerTitre.replacement": "****",
  "transforms.ajouterSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.ajouterSource.static.field": "source",
  "transforms.ajouterSource.static.value": "formation-kafka",
  "transforms.renommer.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
  "transforms.renommer.renames": "DateFilm:annee",
  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true",
  "errors.deadletterqueue.topic.name": "dlq-films",
  "errors.deadletterqueue.topic.replication.factor": "3",
  "errors.deadletterqueue.context.headers.enable": "true"
}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/films-smt/config \
  --data @connect/connectors/file-sink-films-smt.json | jq -r .name
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic films-json < data/films-json.txt
sleep 5
cat connect/data/output/films-smt.txt
```

```text
films-smt
Struct{annee=2022,TitreFilm=****,source=formation-kafka}
Struct{annee=1999,TitreFilm=****,source=formation-kafka}
Struct{annee=2010,TitreFilm=****,source=formation-kafka}
```

| SMT | Effet |
|---|---|
| `MaskField$Value` | remplace `TitreFilm` par `****` (anonymisation) |
| `InsertField$Value` | ajoute le champ statique `source` |
| `ReplaceField$Value` | renomme `DateFilm` en `annee` (aussi : `include`, `exclude`) |

Autres SMT fournies : `Cast`, `TimestampConverter`, `ExtractField`, `HoistField`, `ValueToKey`, `Filter` +
prédicats, `RegexRouter`, `TimestampRouter`, `InsertHeader`, `DropHeaders`, `HeaderFrom`, `Flatten`.

### C.6 Gestion des erreurs et Dead Letter Queue

Messages invalides (pas de JSON / JSON sans enveloppe de schéma) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p data
cat > data/films-invalid.txt <<'EOF_KAFKA_KRAFT'
ceci n'est pas du JSON
{"payload": "JSON sans schéma alors que schemas.enable=true"}
EOF_KAFKA_KRAFT
```

```bash
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic films-json < data/films-invalid.txt
sleep 5
curl -s localhost:8083/connectors/films-smt/status | jq -r '.tasks[0].state'
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic dlq-films --from-beginning --timeout-ms 5000
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic dlq-films --from-beginning --timeout-ms 5000 \
  --formatter-property print.headers=true | tr ',' '\n' | grep "exception.message"
```

```text
RUNNING
ceci n'est pas du JSON
{"payload": "JSON sans schéma alors que schemas.enable=true"}
Processed a total of 2 messages
__connect.errors.exception.message:Converting byte[] to Kafka Connect data failed due to serialization error: 
__connect.errors.exception.message:JsonConverter with schemas.enable requires "schema" and "payload" fields and may not contain additional fields. If you are trying to deserialize plain JSON data
```

Grâce à `errors.tolerance=all`, la tâche reste **RUNNING** : les messages en erreur sont routés vers
`dlq-films` avec leur cause dans les en-têtes (`errors.deadletterqueue.context.headers.enable=true`).
Sans tolérance (`none`, défaut), la tâche passe **FAILED** au premier message invalide.

Supprimez les connecteurs de démonstration :

```bash
for c in films-source films-sink films-smt; do curl -s -X DELETE localhost:8083/connectors/$c; done
curl -s localhost:8083/connectors
```

---

## Partie D – Interface Kafka UI

<http://localhost:8080> → *Kafka Connect* : liste des connecteurs, état des tâches, configuration,
redémarrage. Les topics `_connect-*` et `dlq-films` sont visibles dans *Topics*.

---

## Partie E – Générer des données de test (ex-TP28)

Pour remplir rapidement un topic (benchmark, démonstrations) sans écrire de code :

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic test-benchmark --partitions 3 --replication-factor 3
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic test-benchmark --num-records 1000 --record-size 100 --throughput 100
kafka-producer-perf-test.sh --bootstrap-server $BOOTSTRAP --topic test-benchmark --num-records 1000 --throughput 100 \
  --payload-file ~/KAFKA-KRAFT/data/words.txt --record-key-range 10 --key-distribution random
```

> `--payload-file` tire chaque message au hasard parmi les lignes du fichier ; `--record-key-range` et
> `--key-distribution` (Kafka 4.4) génèrent des clés. Connecteur dédié : *kafka-connect-datagen* (Confluent).

---

## Partie F – Intégration de données : PostgreSQL → Kafka → PostgreSQL (CDC avec Debezium)

```text
 PostgreSQL "formation"                     Kafka                                  PostgreSQL "formation"
 inventory.customers ──WAL (pgoutput)──► [Debezium Postgres source] ──► pg.inventory.customers ──► [Debezium JDBC sink] ──► public.replica_customers
   INSERT / UPDATE / DELETE                 snapshot initial + flux continu         (événements avant/après)      upsert / delete
```

### F.1 Base source

```bash
cd ~/KAFKA-KRAFT
mkdir -p config/postgres
cat > config/postgres/init.sql <<'EOF_KAFKA_KRAFT'
-- =============================================================================
--  Base "formation" : données sources du TP Kafka Connect (CDC Debezium)
--  Exécuté automatiquement au premier démarrage du conteneur postgres
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE inventory.customers (
    id          SERIAL PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(200) NOT NULL UNIQUE,
    city        VARCHAR(100),
    loyalty     VARCHAR(20)  NOT NULL DEFAULT 'STANDARD'
);

-- Images "avant" complètes dans les événements UPDATE / DELETE
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;

INSERT INTO inventory.customers (first_name, last_name, email, city, loyalty) VALUES
  ('Alice',   'Martin',   'alice.martin@example.com',   'Paris',     'GOLD'),
  ('Bruno',   'Bernard',  'bruno.bernard@example.com',  'Lyon',      'STANDARD'),
  ('Chloé',   'Dubois',   'chloe.dubois@example.com',   'Marseille', 'SILVER'),
  ('David',   'Thomas',   'david.thomas@example.com',   'Toulouse',  'STANDARD'),
  ('Emma',    'Robert',   'emma.robert@example.com',    'Nantes',    'GOLD'),
  ('Farid',   'Richard',  'farid.richard@example.com',  'Lille',     'STANDARD'),
  ('Gaëlle',  'Petit',    'gaelle.petit@example.com',   'Rennes',    'SILVER'),
  ('Hugo',    'Durand',   'hugo.durand@example.com',    'Bordeaux',  'STANDARD');
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
docker compose exec postgres psql -U postgres -d formation -c "SHOW wal_level;" -c "SELECT id, first_name, city, loyalty FROM inventory.customers;"
```

```text
 wal_level
-----------
 logical
 id | first_name |   city    | loyalty
----+------------+-----------+----------
  1 | Alice      | Paris     | GOLD
...
(8 rows)
```

### F.2 Télécharger les plugins Debezium

```bash
cd ~/KAFKA-KRAFT
mkdir -p scripts
cat > scripts/download-connect-plugins.sh <<'EOF_KAFKA_KRAFT'
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
EOF_KAFKA_KRAFT
chmod +x scripts/download-connect-plugins.sh
```

```bash
cd ~/KAFKA-KRAFT
./scripts/download-connect-plugins.sh
docker compose --profile connect restart connect
sleep 45
curl -s localhost:8083/connector-plugins | jq -r '.[].class' | grep -i debezium
```

```text
io.debezium.connector.jdbc.JdbcSinkConnector
io.debezium.connector.postgresql.PostgresConnector
```

### F.3 Connecteur source CDC

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/connectors
cat > connect/connectors/pg-source-customers.json <<'EOF_KAFKA_KRAFT'
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "tasks.max": "1",
  "database.hostname": "postgres",
  "database.port": "5432",
  "database.user": "postgres",
  "database.password": "postgres",
  "database.dbname": "formation",
  "topic.prefix": "pg",
  "plugin.name": "pgoutput",
  "slot.name": "debezium_formation",
  "publication.name": "dbz_publication",
  "publication.autocreate.mode": "filtered",
  "table.include.list": "inventory.customers",
  "snapshot.mode": "initial",
  "topic.creation.default.replication.factor": "3",
  "topic.creation.default.partitions": "3"
}
EOF_KAFKA_KRAFT
```

```bash
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/pg-customers-source/config \
  --data @connect/connectors/pg-source-customers.json | jq -r .name
sleep 15
curl -s localhost:8083/connectors/pg-customers-source/status | jq -r '.connector.state, .tasks[0].state'
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic pg.inventory.customers | head -1
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic pg.inventory.customers --from-beginning --max-messages 1 \
  | jq '.payload | {op, before, after, source: {table: .source.table, snapshot: .source.snapshot}}'
```

```text
pg-customers-source
RUNNING
RUNNING
Topic: pg.inventory.customers	TopicId: ...	PartitionCount: 3	ReplicationFactor: 3	...
{
  "op": "r",
  "before": null,
  "after": { "id": 1, "first_name": "Alice", "last_name": "Martin", "email": "alice.martin@example.com", "city": "Paris", "loyalty": "GOLD" },
  "source": { "table": "customers", "snapshot": "first" }
}
```

> `op` : `r` = lecture du *snapshot* initial, `c` = création, `u` = mise à jour, `d` = suppression.
> Le topic est créé par Connect grâce à `topic.creation.default.*` (la création automatique côté broker
> est désactivée). La clé du message est la clé primaire (`{"id": 1}`).

Modifications en base, capturées **en temps réel** :

```bash
docker compose exec postgres psql -U postgres -d formation \
  -c "UPDATE inventory.customers SET city='Grenoble', loyalty='GOLD' WHERE id=2;" \
  -c "INSERT INTO inventory.customers (first_name,last_name,email,city) VALUES ('Inès','Moreau','ines.moreau@example.com','Nice');" \
  -c "DELETE FROM inventory.customers WHERE id=8;"
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic pg.inventory.customers --from-beginning --timeout-ms 8000 \
  | jq -c 'select(.payload != null) | .payload | {op, id: (.after.id // .before.id), before_city: .before.city, after_city: .after.city}' | tail -4
```

```text
{"op":"u","id":2,"before_city":"Lyon","after_city":"Grenoble"}    <- "before" complet grâce à REPLICA IDENTITY FULL
{"op":"c","id":9,"before_city":null,"after_city":"Nice"}
{"op":"d","id":8,"before_city":"Bordeaux","after_city":null}
```

> Après un `DELETE`, Debezium émet aussi un message de valeur `null` (*tombstone*) pour la compaction :
> d'où le filtre `select(.payload != null)`.

### F.4 Connecteur sink JDBC : réplication vers une table cible

```bash
cd ~/KAFKA-KRAFT
mkdir -p connect/connectors
cat > connect/connectors/jdbc-sink-customers.json <<'EOF_KAFKA_KRAFT'
{
  "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
  "tasks.max": "1",
  "topics": "pg.inventory.customers",
  "connection.url": "jdbc:postgresql://postgres:5432/formation",
  "connection.username": "postgres",
  "connection.password": "postgres",
  "insert.mode": "upsert",
  "delete.enabled": "true",
  "primary.key.mode": "record_key",
  "schema.evolution": "basic",
  "collection.name.format": "replica_${source.table}",
  "use.time.zone": "UTC"
}
EOF_KAFKA_KRAFT
```

```bash
curl -s -X PUT -H "Content-Type: application/json" localhost:8083/connectors/jdbc-customers-sink/config \
  --data @connect/connectors/jdbc-sink-customers.json | jq -r .name
sleep 15
curl -s localhost:8083/connectors/jdbc-customers-sink/status | jq -r '.connector.state, .tasks[0].state'
docker compose exec postgres psql -U postgres -d formation -c "SELECT id, first_name, city, loyalty FROM public.replica_customers ORDER BY id;"
```

```text
 id | first_name |   city    | loyalty
----+------------+-----------+----------
  1 | Alice      | Paris     | GOLD
  2 | Bruno      | Grenoble  | GOLD
  ...
  9 | Inès       | Nice      | STANDARD
(8 rows)                                  <- le client 8 supprimé n'est pas présent
```

La table cible a été **créée automatiquement** (`schema.evolution=basic`), alimentée en **upsert** et
les suppressions sont propagées (`delete.enabled=true`). Testez la synchronisation continue :

```bash
docker compose exec postgres psql -U postgres -d formation -c "UPDATE inventory.customers SET loyalty='SILVER' WHERE id=4;"
sleep 5
docker compose exec postgres psql -U postgres -d formation -c "SELECT id, first_name, loyalty FROM public.replica_customers WHERE id=4;"
```

### F.5 Exploitation des connecteurs

```bash
curl -s -X PUT localhost:8083/connectors/pg-customers-source/pause
curl -s localhost:8083/connectors/pg-customers-source/status | jq -r .connector.state
curl -s -X PUT localhost:8083/connectors/pg-customers-source/resume
curl -s -X POST "localhost:8083/connectors/jdbc-customers-sink/restart?includeTasks=true&onlyFailed=true" | jq
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group connect-jdbc-customers-sink
```

```text
PAUSED
...
GROUP                        TOPIC                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG ...
connect-jdbc-customers-sink  pg.inventory.customers   0          ...             ...             0
```

> Un connecteur **sink** est un consumer group nommé `connect-<nom du connecteur>` : son lag se surveille
> comme celui de n'importe quelle application (TP05).

---

## Partie G – Fin du TP

```bash
cd ~/KAFKA-KRAFT
for c in pg-customers-source jdbc-customers-sink; do curl -s -X DELETE localhost:8083/connectors/$c; done
sleep 10
docker compose exec postgres psql -U postgres -d formation -c "SELECT pg_drop_replication_slot('debezium_formation');"
docker compose --profile connect stop
```

> Supprimer le slot de réplication est important : un slot inutilisé empêche PostgreSQL de recycler ses
> journaux WAL (le disque se remplit).

### Ce qu'il faut retenir

1. Connect = intégration **sans code** : source → topic → sink, configuration JSON via l'API REST.
2. Mode **distribué** en production (état dans `_connect-*`, RF=3), mode standalone pour les tests.
3. Le **converter** détermine le format dans Kafka ; les **SMT** transforment message par message.
4. `errors.tolerance=all` + **DLQ** pour ne pas bloquer un pipeline sur un message invalide.
5. CDC (Debezium) : capture fiable des changements d'une base via son journal (WAL), sans requêtes intrusives.

### Questions de révision

1. Où sont stockés les offsets d'un connecteur source en mode distribué ? et d'un connecteur sink ?
2. Pourquoi `tasks.max=1` pour le connecteur Debezium PostgreSQL ?
3. Quelle différence entre une SMT et un traitement Kafka Streams ?

✅ **Suite : TP07b – Kafka Streams.**
