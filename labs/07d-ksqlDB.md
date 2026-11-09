# TP07d – ksqlDB : traitement de flux en SQL

> **Chapitre 7 du programme (écosystème)**
> Durée : 45 min
> Pré-requis : cluster du TP02, `source ~/KAFKA-KRAFT/scripts/kafka-env.sh`
> Reprend et met à jour : TP21 (ksqlDB « riders ») → **ksqlDB (Confluent 8.4)** sur Kafka 4.4 KRaft, plus un cas d'usage « commandes »

---

## Objectifs

- Comprendre ce qu'est ksqlDB : une application **Kafka Streams** pilotée en SQL
- Créer des **STREAM** et des **TABLE**, des requêtes **persistantes**, **push** et **pull**
- Filtrer, enrichir, agréger et fenêtrer un flux de commandes sans écrire de Java

---

## Partie A – Rappels

| Objet | Sémantique | Équivalent Kafka Streams |
|---|---|---|
| `STREAM` | suite **immuable** d'événements (insertions seules) | `KStream` |
| `TABLE` | **dernier état** par clé (upsert, tombstone = suppression) | `KTable` |
| Requête persistante `CREATE ... AS SELECT` | tourne en continu sur le serveur, écrit dans un topic | topologie |
| Requête **push** `... EMIT CHANGES` | le client reçoit les changements au fil de l'eau | |
| Requête **pull** (sans `EMIT CHANGES`) | lecture ponctuelle de l'état d'une table matérialisée | *Interactive Queries* |

```text
 topic orders-ksql ──► STREAM orders_stream ──(WHERE quantity > 0)──► STREAM orders_valid (topic orders_valid)
                                                                   ├──(GROUP BY country)──────► TABLE revenue_by_country
                                                                   └──(WINDOW TUMBLING 1 min)─► TABLE orders_per_customer_1m
```

> ksqlDB est distribué par Confluent sous *Confluent Community License* (usage libre, mais pas de
> revente « as a service »). Il est en mode maintenance : pour de nouveaux développements, Kafka
> Streams (TP07b) ou Apache Flink SQL sont à privilégier. Il reste très pratique pour l'exploration.

---

## Partie B – Démarrer ksqlDB

Le profil `ksqldb` démarre aussi le Schema Registry.

```bash
cd ~/KAFKA-KRAFT
docker compose --profile ksqldb up -d
until curl -sf localhost:8088/info >/dev/null; do echo "attente de ksqlDB..."; sleep 5; done
curl -s localhost:8088/info | jq
```

```text
{
  "KsqlServerInfo": {
    "version": "8.4.0",
    "kafkaClusterId": "PIYzDzl8TomQ84Cnrxi0gA",
    "ksqlServiceId": "formation_",
    "serverStatus": "RUNNING"
  }
}
```

Le `kafkaClusterId` est bien celui de notre cluster KRaft (fichier `.env`). ksqlDB stocke ses
définitions dans un topic interne (« command topic ») répliqué 3 fois :

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep -i ksql
```

```text
_confluent-ksql-formation__command_topic
formation_ksql_processing_log
```

---

## Partie C – Premier exemple : « riders » (géolocalisation)

Le script de référence (à copier-coller instruction par instruction dans la CLI) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/ksqldb
cat > data/ksqldb/01-riders.sql <<'EOF_KAFKA_KRAFT'
-- =============================================================================
--  ksqlDB - exemple "riders" (repris du TP21) : flux, table matérialisée,
--  requêtes push et pull. A exécuter dans la CLI ksql.
-- =============================================================================
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)
  WITH (KAFKA_TOPIC='locations', VALUE_FORMAT='JSON', PARTITIONS=1, REPLICAS=3);

CREATE TABLE currentLocation AS
  SELECT profileId,
         LATEST_BY_OFFSET(latitude)  AS la,
         LATEST_BY_OFFSET(longitude) AS lo
  FROM riderLocations
  GROUP BY profileId
  EMIT CHANGES;

CREATE TABLE ridersNearMountainView AS
  SELECT ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1) AS distanceInMiles,
         COLLECT_LIST(profileId) AS riders,
         COUNT(*) AS count
  FROM currentLocation
  GROUP BY ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1);

INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);
EOF_KAFKA_KRAFT
```

Ouvrir la CLI (terminal 1) :

```bash
cd ~/KAFKA-KRAFT
docker compose exec ksqldb-server ksql http://localhost:8088
```

Dans la CLI `ksql>`, coller **dans l'ordre** : le `SET`, le `CREATE STREAM`, puis les deux `CREATE TABLE`.

```sql
SHOW STREAMS;
SHOW TABLES;
```

Ouvrir une **requête push** (elle reste ouverte) :

```sql
SELECT * FROM riderLocations
  WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;
```

Dans un **terminal 2**, ouvrir une seconde CLI et insérer les positions (les 6 `INSERT INTO` du script) :

```bash
cd ~/KAFKA-KRAFT
docker compose exec ksqldb-server ksql http://localhost:8088
```

Le terminal 1 affiche au fil de l'eau les 3 coureurs situés à moins de 5 km :

```text
+-----------+-----------+------------+
|PROFILEID  |LATITUDE   |LONGITUDE   |
+-----------+-----------+------------+
|4ab5cbad   |37.3952    |-122.0813   |
|8b6eae59   |37.3944    |-122.0813   |
|4a7c7b41   |37.4049    |-122.0822   |
```

`Ctrl+C` pour arrêter la requête push. Requêtes **pull** (réponse immédiate puis fin) :

```sql
SELECT * FROM currentLocation WHERE profileId = '4ab5cbad';
SELECT * FROM ridersNearMountainView WHERE distanceInMiles <= 10;
```

```text
+-----------+---------+-----------+
|PROFILEID  |LA       |LO         |
+-----------+---------+-----------+
|4ab5cbad   |37.3952  |-122.0813  |

+----------------+------------------------------+------+
|DISTANCEINMILES |RIDERS                        |COUNT |
+----------------+------------------------------+------+
|0.0             |[4ab5cbad, 8b6eae59, 4a7c7b41]|3     |
|10.0            |[18f4ea86]                    |1     |
```

> `GEO_DISTANCE` calcule en **kilomètres** par défaut (le nom de colonne hérité de l'exemple officiel
> est trompeur). Les deux coureurs de San Francisco (~50 km) n'apparaissent pas avec le filtre `<= 10`.

---

## Partie D – Cas d'usage : analyse des commandes

### D.1 Jeu de données

Les commandes de `data/orders.csv` (60 lignes, dont 2 avec une quantité nulle) sont converties en
messages `clé|JSON` (clé = `customerId`, même format JSON que le topic `orders` des TP Java/Spring) :

```bash
cd ~/KAFKA-KRAFT
tail -n +2 data/orders.csv | awk -F, '{printf "%s|{\"orderId\":\"%s\",\"customerId\":\"%s\",\"product\":\"%s\",\"quantity\":%d,\"unitPrice\":%s,\"country\":\"%s\"}\n",$2,$1,$2,$3,$4,$5,$6}' \
  > data/ksqldb/orders-ksql.txt
wc -l data/ksqldb/orders-ksql.txt && head -2 data/ksqldb/orders-ksql.txt
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic orders-ksql --partitions 3 --replication-factor 3
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic orders-ksql \
  --property parse.key=true --property key.separator='|' < data/ksqldb/orders-ksql.txt
```

```text
60 data/ksqldb/orders-ksql.txt
C-005|{"orderId":"ORD-0001","customerId":"C-005","product":"Souris","quantity":1,"unitPrice":19.9,"country":"BE"}
C-002|{"orderId":"ORD-0002","customerId":"C-002","product":"Casque","quantity":2,"unitPrice":89.0,"country":"IT"}
Created topic orders-ksql.
```

### D.2 Le script SQL

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/ksqldb
cat > data/ksqldb/02-orders.sql <<'EOF_KAFKA_KRAFT'
-- =============================================================================
--  ksqlDB - analyse en SQL d'un flux de commandes JSON (topic "orders-ksql",
--  alimenté à partir de data/orders.csv - même format que le topic "orders"
--  des TP Java/Spring)
-- =============================================================================
SET 'auto.offset.reset' = 'earliest';

-- Flux sur le topic existant (la clé du message = customerId)
CREATE STREAM orders_stream (
    customerId VARCHAR KEY,
    orderId    VARCHAR,
    product    VARCHAR,
    quantity   INT,
    unitPrice  DOUBLE,
    country    VARCHAR
) WITH (KAFKA_TOPIC='orders-ksql', VALUE_FORMAT='JSON');

-- Flux dérivé : commandes valides enrichies du montant (topic ORDERS_VALID créé par ksqlDB)
CREATE STREAM orders_valid WITH (KAFKA_TOPIC='orders_valid', PARTITIONS=3, REPLICAS=3) AS
  SELECT customerId, orderId, product, quantity, unitPrice, country,
         quantity * unitPrice AS amount
  FROM orders_stream
  WHERE quantity > 0
  EMIT CHANGES;

-- Table agrégée : chiffre d'affaires et nombre de commandes par pays
CREATE TABLE revenue_by_country WITH (KAFKA_TOPIC='revenue_by_country', PARTITIONS=3, REPLICAS=3) AS
  SELECT country,
         COUNT(*)            AS nb_orders,
         ROUND(SUM(amount), 2) AS revenue
  FROM orders_valid
  GROUP BY country
  EMIT CHANGES;

-- Fenêtre fixe ("tumbling") de 1 minute : nombre de commandes par client
CREATE TABLE orders_per_customer_1m WITH (KAFKA_TOPIC='orders_per_customer_1m', PARTITIONS=3, REPLICAS=3) AS
  SELECT customerId,
         COUNT(*) AS nb_orders
  FROM orders_valid
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY customerId
  EMIT CHANGES;
EOF_KAFKA_KRAFT
```

Dans la CLI (terminal 1), d'abord inspecter le topic brut :

```sql
PRINT 'orders-ksql' FROM BEGINNING LIMIT 2;
```

```text
Key format: KAFKA_STRING
Value format: JSON or KAFKA_STRING
rowtime: 2026/11/09 09:12:03.418 Z, key: C-002, value: {"orderId":"ORD-0002","customerId":"C-002",...}, partition: 0
rowtime: 2026/11/09 09:12:03.419 Z, key: C-007, value: {"orderId":"ORD-0006","customerId":"C-007",...}, partition: 0
Topic printing ceased
```

Puis coller **dans l'ordre** les instructions du script (`SET`, `CREATE STREAM orders_stream`,
`CREATE STREAM orders_valid`, `CREATE TABLE revenue_by_country`, `CREATE TABLE orders_per_customer_1m`).

```sql
SHOW QUERIES;
DESCRIBE orders_valid;
```

```text
 Query ID                          | Query Type | Status    | Sink Name              | Sink Kafka Topic       | Query String
----------------------------------------------------------------------------------------------------------------------------
 CSAS_ORDERS_VALID_...             | PERSISTENT | RUNNING:1 | ORDERS_VALID           | orders_valid           | CREATE STREAM ...
 CTAS_REVENUE_BY_COUNTRY_...       | PERSISTENT | RUNNING:1 | REVENUE_BY_COUNTRY     | revenue_by_country     | CREATE TABLE ...
 CTAS_ORDERS_PER_CUSTOMER_1M_...   | PERSISTENT | RUNNING:1 | ORDERS_PER_CUSTOMER_1M | orders_per_customer_1m | CREATE TABLE ...
```

### D.3 Requêtes push et pull

```sql
SELECT orderId, customerId, amount FROM orders_valid EMIT CHANGES LIMIT 3;
SELECT * FROM revenue_by_country;
SELECT * FROM revenue_by_country WHERE country = 'FR';
SELECT customerId, WINDOWSTART, WINDOWEND, nb_orders FROM orders_per_customer_1m WHERE customerId = 'C-008';
```

Résultat attendu pour l'agrégat (58 commandes valides, ORD-0017 et ORD-0042 filtrées) :

```text
+--------+-----------+---------+
|COUNTRY |NB_ORDERS  |REVENUE  |
+--------+-----------+---------+
|BE      |13         |5951.0   |
|CA      |5          |1016.9   |
|CH      |2          |1601.0   |
|DE      |5          |9504.5   |
|ES      |4          |1311.6   |
|FR      |20         |20936.4  |
|IT      |9          |3216.6   |
```

> L'ordre des lignes d'une table peut varier. La fenêtre d'une minute de `C-008` contient ses
> 9 commandes (toutes produites dans la même seconde ; si l'envoi chevauche deux minutes, deux
> fenêtres apparaissent).

### D.4 L'état évolue en continu

Garder une requête push ouverte sur la table (terminal 1) :

```sql
SELECT * FROM revenue_by_country WHERE country = 'FR' EMIT CHANGES;
```

Dans le terminal 2 (CLI ksql), insérer une commande, puis une commande invalide :

```sql
INSERT INTO orders_stream (customerId, orderId, product, quantity, unitPrice, country)
  VALUES ('C-001', 'KSQL-0001', 'Laptop', 2, 1000.0, 'FR');
INSERT INTO orders_stream (customerId, orderId, product, quantity, unitPrice, country)
  VALUES ('C-001', 'KSQL-0002', 'Laptop', 0, 1000.0, 'FR');
```

```text
|FR      |21         |22936.4  |
```

Seule la première commande modifie le chiffre d'affaires. Le résultat est aussi visible depuis Kafka :

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic revenue_by_country --from-beginning \
  --property print.key=true --timeout-ms 5000 2>/dev/null | grep '^FR' | tail -2
```

```text
FR	{"NB_ORDERS":20,"REVENUE":20936.4}
FR	{"NB_ORDERS":21,"REVENUE":22936.4}
```

> Le topic de sortie d'une `TABLE` est un **changelog** : une ligne par mise à jour de la clé
> (d'où `tail -2` : les mises à jour intermédiaires `NB_ORDERS` 1, 2, 3… précèdent). Les noms de
> champs JSON écrits par ksqlDB sont en **majuscules** (identifiants SQL non quotés).

Kafka UI (<http://localhost:8080>) : menu **KSQL DB** (streams, tables, requêtes, exécution SQL).

---

## Partie E – Nettoyage

Dans la CLI (la suppression d'un objet arrête la requête qui l'alimente) :

```sql
DROP TABLE IF EXISTS orders_per_customer_1m DELETE TOPIC;
DROP TABLE IF EXISTS revenue_by_country DELETE TOPIC;
DROP STREAM IF EXISTS orders_valid DELETE TOPIC;
DROP STREAM IF EXISTS orders_stream;
DROP TABLE IF EXISTS ridersNearMountainView DELETE TOPIC;
DROP TABLE IF EXISTS currentLocation DELETE TOPIC;
DROP STREAM IF EXISTS riderLocations DELETE TOPIC;
SHOW QUERIES;
exit
```

```bash
cd ~/KAFKA-KRAFT
docker compose --profile ksqldb stop ksqldb-server schema-registry
```

### Ce qu'il faut retenir

1. `STREAM` = événements, `TABLE` = état ; une requête `CREATE ... AS SELECT` est une application Kafka Streams.
2. Push (`EMIT CHANGES`) pour suivre en continu, pull pour lire l'état courant d'une table matérialisée.
3. Les tables d'agrégat sont stockées localement (RocksDB) et sauvegardées dans des topics changelog.
4. Pour la production : Kafka Streams / Spring Kafka Streams (TP07b) ou Flink SQL.

### Questions de révision

1. Pourquoi la commande `KSQL-0002` ne modifie-t-elle pas `revenue_by_country` ?
2. Quelle est la différence entre une requête push et une requête pull ?
3. Où ksqlDB stocke-t-il la définition des streams et tables ?

✅ **Suite : TP07e – MirrorMaker 2.**
