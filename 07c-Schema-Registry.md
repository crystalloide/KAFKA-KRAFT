# TP07c – Schema Registry : contrats de données et évolution des schémas

> **Chapitre 7 du programme (écosystème)**
> Durée : 45 min
> Pré-requis : cluster du TP02, `source ~/KAFKA-KRAFT/scripts/kafka-env.sh`
> Reprend et met à jour : TP26 (Schema Registry 7.x + ZooKeeper → **Confluent Schema Registry 8.4** sur Kafka 4.4 KRaft)

---

## Objectifs

- Comprendre le rôle du **Schema Registry** et le format des messages sérialisés
- Enregistrer et faire **évoluer** un schéma Avro selon une règle de **compatibilité**
- Produire et consommer des messages **Avro** contrôlés par le registre
- Connaître l'intégration côté Java / Spring Boot

---

## Partie A – Rappels

```text
 Producer (KafkaAvroSerializer)                       Schema Registry (topic _schemas)            Consumer (KafkaAvroDeserializer)
  1. schéma -> registre (si inconnu) ─────────────►  sujet "orders-avro-value" : v1 (id 1), v2 (id 2)  ◄──── 3. id -> schéma (mis en cache)
  2. message = [0x00][id sur 4 octets][données Avro binaires] ──────────► topic orders-avro ──────────►  4. désérialisation
```

| Notion | Description |
|---|---|
| Sujet (*subject*) | nom sous lequel les versions d'un schéma sont rangées ; par défaut `<topic>-key` / `<topic>-value` (*TopicNameStrategy*) |
| Formats | **Avro**, **JSON Schema**, **Protobuf** |
| Compatibilité | contrôlée à chaque nouvel enregistrement de version |

| Mode | Règle | Ordre de déploiement |
|---|---|---|
| `BACKWARD` (défaut) | le **nouveau** schéma sait lire les données de l'ancien | consommateurs d'abord |
| `FORWARD` | l'**ancien** schéma sait lire les données du nouveau | producteurs d'abord |
| `FULL` | les deux | indifférent |
| `*_TRANSITIVE` | vérifié par rapport à **toutes** les versions | |
| `NONE` | aucun contrôle | à éviter |

Ajouter un champ **avec valeur par défaut** est compatible ; ajouter un champ obligatoire **sans défaut**
casse la compatibilité `BACKWARD` ; renommer = supprimer + ajouter (utiliser les *aliases* Avro).

---

## Partie B – Démarrer le Schema Registry

```bash
cd ~/KAFKA-KRAFT
docker compose --profile schema up -d schema-registry
sleep 30
curl -s localhost:8081/config | jq
curl -s localhost:8081/subjects | jq
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic _schemas | head -1
```

```text
{ "compatibilityLevel": "BACKWARD" }
[]
Topic: _schemas	TopicId: ...	PartitionCount: 1	ReplicationFactor: 3	Configs: cleanup.policy=compact
```

> Les schémas sont stockés dans le topic compacté `_schemas` (1 partition, RF=3) : le registre est
> lui-même sans état et peut être redémarré ou multiplié (une instance « leader » pour les écritures).

---

## Partie C – Enregistrer et faire évoluer un schéma (API REST)

### C.1 Les schémas

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/avro
cat > data/avro/order-v1.avsc <<'EOF_KAFKA_KRAFT'
{
  "type": "record",
  "name": "Order",
  "namespace": "fr.formation.kafka.avro",
  "doc": "Commande - version 1",
  "fields": [
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "product",    "type": "string"},
    {"name": "quantity",   "type": "int"},
    {"name": "unitPrice",  "type": "double"}
  ]
}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/avro
cat > data/avro/order-v2.avsc <<'EOF_KAFKA_KRAFT'
{
  "type": "record",
  "name": "Order",
  "namespace": "fr.formation.kafka.avro",
  "doc": "Commande - version 2 : ajout d'un champ optionnel AVEC valeur par défaut (compatible BACKWARD)",
  "fields": [
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "product",    "type": "string"},
    {"name": "quantity",   "type": "int"},
    {"name": "unitPrice",  "type": "double"},
    {"name": "country",    "type": "string", "default": "FR"}
  ]
}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/avro
cat > data/avro/order-v3-incompatible.avsc <<'EOF_KAFKA_KRAFT'
{
  "type": "record",
  "name": "Order",
  "namespace": "fr.formation.kafka.avro",
  "doc": "Commande - version 3 : ajout d'un champ obligatoire SANS valeur par défaut (INCOMPATIBLE BACKWARD)",
  "fields": [
    {"name": "orderId",    "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "product",    "type": "string"},
    {"name": "quantity",   "type": "int"},
    {"name": "unitPrice",  "type": "double"},
    {"name": "country",    "type": "string", "default": "FR"},
    {"name": "vatRate",    "type": "double"}
  ]
}
EOF_KAFKA_KRAFT
```

### C.2 Enregistrement de la version 1

```bash
cd ~/KAFKA-KRAFT
SR=localhost:8081
CT="Content-Type: application/vnd.schemaregistry.v1+json"
jq -n --rawfile schema data/avro/order-v1.avsc '{schema: $schema}' \
  | curl -s -X POST -H "$CT" --data @- $SR/subjects/orders-avro-value/versions | jq
curl -s $SR/subjects | jq
curl -s $SR/subjects/orders-avro-value/versions/latest | jq '{subject, version, id}'
```

```text
{ "id": 1 }
[ "orders-avro-value" ]
{ "subject": "orders-avro-value", "version": 1, "id": 1 }
```

### C.3 Tester la compatibilité avant d'enregistrer

```bash
jq -n --rawfile schema data/avro/order-v2.avsc '{schema: $schema}' \
  | curl -s -X POST -H "$CT" --data @- "$SR/compatibility/subjects/orders-avro-value/versions/latest" | jq
jq -n --rawfile schema data/avro/order-v3-incompatible.avsc '{schema: $schema}' \
  | curl -s -X POST -H "$CT" --data @- "$SR/compatibility/subjects/orders-avro-value/versions/latest?verbose=true" | jq
```

```text
{ "is_compatible": true }
{
  "is_compatible": false,
  "messages": [ "{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field 'vatRate' at path '/fields/6' in the new schema has no default value and is missing in the old schema', ...}" ]
}
```

### C.4 Enregistrer la version 2, refuser la version 3

```bash
jq -n --rawfile schema data/avro/order-v2.avsc '{schema: $schema}' \
  | curl -s -X POST -H "$CT" --data @- $SR/subjects/orders-avro-value/versions | jq
jq -n --rawfile schema data/avro/order-v3-incompatible.avsc '{schema: $schema}' \
  | curl -s -X POST -H "$CT" --data @- $SR/subjects/orders-avro-value/versions | jq
curl -s $SR/subjects/orders-avro-value/versions | jq -c
curl -s $SR/schemas/ids/2 | jq -r .schema | jq '.fields[-1]'
```

```text
{ "id": 2 }
{ "error_code": 409, "message": "Schema being registered is incompatible with an earlier schema for subject \"orders-avro-value\", details: [...]" }
[1,2]
{ "name": "country", "type": "string", "default": "FR" }
```

Compatibilité **par sujet** (prioritaire sur la valeur globale) :

```bash
curl -s -X PUT -H "$CT" --data '{"compatibility": "FULL"}' $SR/config/orders-avro-value | jq
curl -s $SR/config/orders-avro-value | jq
```

---

## Partie D – Produire et consommer en Avro

Les outils `kafka-avro-console-producer` / `-consumer` sont fournis dans l'image du Schema Registry
(le répertoire `data/avro` y est monté en lecture seule sur `/data/avro`).

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/avro
cat > data/avro/orders-v1.jsonl <<'EOF_KAFKA_KRAFT'
{"orderId": "AVRO-0001", "customerId": "C-001", "product": "Clavier", "quantity": 2, "unitPrice": 49.9}
{"orderId": "AVRO-0002", "customerId": "C-002", "product": "Ecran 27p", "quantity": 1, "unitPrice": 289.0}
{"orderId": "AVRO-0003", "customerId": "C-001", "product": "Souris", "quantity": 3, "unitPrice": 19.9}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p data/avro
cat > data/avro/orders-v2.jsonl <<'EOF_KAFKA_KRAFT'
{"orderId": "AVRO-0004", "customerId": "C-003", "product": "Casque", "quantity": 1, "unitPrice": 89.0, "country": "BE"}
{"orderId": "AVRO-0005", "customerId": "C-004", "product": "Webcam", "quantity": 2, "unitPrice": 59.0, "country": "CH"}
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists --topic orders-avro --partitions 3 --replication-factor 3
docker compose exec -T schema-registry kafka-avro-console-producer --bootstrap-server broker-1:19092 --topic orders-avro \
  --property schema.registry.url=http://localhost:8081 --property value.schema.file=/data/avro/order-v1.avsc < data/avro/orders-v1.jsonl
docker compose exec -T schema-registry kafka-avro-console-producer --bootstrap-server broker-1:19092 --topic orders-avro \
  --property schema.registry.url=http://localhost:8081 --property value.schema.file=/data/avro/order-v2.avsc < data/avro/orders-v2.jsonl
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server broker-1:19092 --topic orders-avro \
  --from-beginning --max-messages 5 --property schema.registry.url=http://localhost:8081
```

```text
{"orderId":"AVRO-0001","customerId":"C-001","product":"Clavier","quantity":2,"unitPrice":49.9}
{"orderId":"AVRO-0004","customerId":"C-003","product":"Casque","quantity":1,"unitPrice":89.0,"country":"BE"}
...
Processed a total of 5 messages
```

> L'ordre d'affichage dépend des partitions. Les messages v1 sont relus **sans** `country` (schéma
> d'écriture v1), les messages v2 avec. Depuis Kafka 4.2, l'option `--property` des outils console est
> dépréciée (au profit de `--reader-property` / `--formatter-property`) : un avertissement peut s'afficher,
> sans conséquence.

Un message qui ne respecte pas le schéma est refusé **par le producer** (avant d'atteindre Kafka) :

```bash
echo '{"orderId": "AVRO-9999", "customerId": "C-001"}' | docker compose exec -T schema-registry kafka-avro-console-producer \
  --bootstrap-server broker-1:19092 --topic orders-avro --property schema.registry.url=http://localhost:8081 \
  --property value.schema.file=/data/avro/order-v2.avsc
```

```text
org.apache.kafka.common.errors.SerializationException: Error deserializing json {"orderId": "AVRO-9999", ...} to Avro of schema {...}
...
Caused by: org.apache.avro.AvroTypeException: Expected field name not found: product
```

Le format « fil » d'un message Avro (octet magique `0`, puis l'**id du schéma** sur 4 octets, puis les
données binaires) — on lit le message brut sans désérialiseur Avro :

```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic orders-avro --from-beginning --max-messages 1 2>/dev/null \
  | od -A d -t x1 -N 16
```

```text
0000000 00 00 00 00 01 12 41 56 52 4f 2d 30 30 30 31 0a
```

Lecture : `00` = octet magique, `00 00 00 01` = schéma **id 1** (ou `00 00 00 02` si le premier message lu
est un message v2), `12` = longueur de la chaîne `orderId` (9 caractères, codage *zigzag*), puis `AVRO-0001`.

Kafka UI (<http://localhost:8080>) : menu *Schema Registry* (sujets, versions, compatibilité) et
*Topics → orders-avro → Messages* (désérialisation Avro automatique).

---

## Partie E – Côté application Java / Spring Boot (référence)

Dépendance (dépôt Maven Confluent `https://packages.confluent.io/maven/`) :

```xml
<repositories>
  <repository>
    <id>confluent</id>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>
<dependency>
  <groupId>io.confluent</groupId>
  <artifactId>kafka-avro-serializer</artifactId>
  <version>8.4.0</version>
</dependency>
```

Configuration Spring Boot :

```yaml
spring:
  kafka:
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true
    properties:
      schema.registry.url: http://localhost:8081
      auto.register.schemas: false      # en production : schémas enregistrés par la CI, pas par les applications
      use.latest.version: true
```

Les classes Java sont générées à partir des `.avsc` par le plugin `avro-maven-plugin`.
Alternatives sans Avro : `KafkaJsonSchemaSerializer` (JSON Schema), `KafkaProtobufSerializer`.

---

## Partie F – Fin du TP

```bash
cd ~/KAFKA-KRAFT
docker compose --profile schema stop schema-registry    # ou le laisser pour le TP07d (ksqlDB)
```

### Ce qu'il faut retenir

1. Le Schema Registry est le **contrat** entre producteurs et consommateurs ; le message ne contient que l'**id** du schéma.
2. La règle de compatibilité (BACKWARD par défaut) est vérifiée **à l'enregistrement** d'une nouvelle version.
3. Champ ajouté = valeur par défaut obligatoire (BACKWARD) ; tester avec `/compatibility` dans la CI.
4. En production : `auto.register.schemas=false`, schémas versionnés dans Git.

### Questions de révision

1. Quel mode de compatibilité choisir si les producteurs sont mis à jour avant les consommateurs ?
2. Pourquoi le topic `_schemas` est-il compacté ?
3. Que contiennent les 5 premiers octets d'un message Avro produit avec le Schema Registry ?

✅ **Suite : TP07d – ksqlDB.**
