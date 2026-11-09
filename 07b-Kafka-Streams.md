# TP07b – Kafka Streams : traitement de flux

> **Chapitre 7 du programme (écosystème)** – Objectif pédagogique n°6
> Durée : 1 h 30
> Pré-requis : TP03 (projet `java/kafka-java-native` compilé), topics créés par `scripts/create-topics.sh`
> Reprend et met à jour : TP16 (basique), TP17 (filtrage), TP18 (mapValues), TP19 (flatMap), TP20 (foreach)
> – API Kafka Streams **4.4.0** ; ajout du comptage avec état, des fenêtres, d'un cas métier et de Spring

---

## Objectifs

- Comprendre le modèle **KStream / KTable**, la **topologie** et l'`application.id`
- Mettre en oeuvre les opérations **sans état** : `filter`, `mapValues`, `flatMap`, `peek`, `foreach`, `split`
- Mettre en oeuvre les opérations **avec état** : `groupBy`, `count`, `reduce`, **fenêtres**
- Comprendre les **topics internes** (repartition, changelog), la **scalabilité** et la **tolérance aux pannes**
- Utiliser Kafka Streams avec **Spring for Apache Kafka**

---

## Partie A – Rappels

```text
              application.id = "wordcount-app"  (= group.id + préfixe des topics internes)
 words-input ──► flatMapValues ──► groupBy(mot) ──► [wordcount-app-word-counts-repartition] ──► count()
  (KStream)       (sans état)       (nouvelle clé)          topic interne                       │ état RocksDB local
                                                                                                │ + [wordcount-app-word-counts-changelog]
                                                                                                ▼
                                                                         KTable<mot, total> ──► toStream() ──► words-output
```

| Notion | Description |
|---|---|
| **KStream** | flux d'événements (chaque message est un fait indépendant) |
| **KTable** | table de changements (dernière valeur par clé) ; **GlobalKTable** : copie complète sur chaque instance |
| Topologie | graphe de processeurs construit avec `StreamsBuilder` (DSL) ou la Processor API |
| Sans état | `filter`, `map`, `mapValues`, `flatMap`, `peek`, `foreach`, `split`, `merge` |
| Avec état | `groupBy`/`groupByKey` + `count`, `reduce`, `aggregate`, jointures, fenêtres |
| State store | stockage local (RocksDB) **sauvegardé** dans un topic *changelog* (restauration après panne) |
| Repartition | changement de clé avant un agrégat ⇒ topic interne `-repartition` |
| Parallélisme | 1 **tâche** par partition d'entrée ; instances et threads (`num.stream.threads`) se partagent les tâches |
| Garantie | `processing.guarantee=exactly_once_v2` (transactions) |
| Fenêtres | *tumbling* (fixes), *hopping* (glissantes à pas fixe), *sliding*, *session* |

> Kafka 4.x : les anciennes API `KStream#branch`, `transform`, `through`... ont été **supprimées**
> (remplacées par `split()`, `process()`, `repartition()`). Nouveau protocole de rééquilibrage dédié aux
> applications Streams (KIP-1071, `group.protocol=streams`), production-ready depuis Kafka 4.2.

---

## Partie B – Opérations sans état (ex-TP16 à TP20)

Code commun de démarrage (configuration, gestion des erreurs, arrêt propre) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsSupport.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Démarrage commun des applications Kafka Streams des TP. */
final class StreamsSupport {

    private StreamsSupport() {
    }

    /**
     * application.id = identifiant de l'application ; il sert de group.id, de préfixe
     * aux topics internes (changelog, repartition) et au répertoire d'état local.
     */
    static Properties config(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // relit le topic d'entrée depuis le début au premier démarrage
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        return props;
    }

    static void run(StreamsBuilder builder, Properties props) {
        Topology topology = builder.build();
        System.out.println("Topologie :\n" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("Erreur fatale du traitement : " + exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.setStateListener((newState, oldState) ->
                System.out.println("Etat Kafka Streams : " + oldState + " -> " + newState));

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
EOF_KAFKA_KRAFT
```

Pour chaque exercice : **terminal 1** = application Streams, **terminal 2** = producteur console,
**terminal 3** = consommateur du topic de sortie.

```bash
# Terminal 2
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic streams-input \
  --reader-property parse.key=true --reader-property key.separator=:
```

```bash
# Terminal 3
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic streams-output --formatter-property print.key=true
```

### B.1 Copie simple d'un topic vers un autre

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsPassThrough.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * TP Kafka Streams 1 - copie simple d'un topic vers un autre (sans transformation).
 * Usage : ./run.sh streams.StreamsPassThrough
 */
public class StreamsPassThrough {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-input");
        source.print(Printed.<String, String>toSysOut().withLabel("lu"));
        source.to("streams-output", Produced.with(Serdes.String(), Serdes.String()));
        StreamsSupport.run(builder, StreamsSupport.config("streams-passthrough"));
    }
}
EOF_KAFKA_KRAFT
```

```bash
# Terminal 1
cd ~/KAFKA-KRAFT/java/kafka-java-native && mvn -q -DskipTests package
./run.sh streams.StreamsPassThrough
```

```text
Topologie :
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [streams-input])
      --> KSTREAM-PRINTER-0000000001, KSTREAM-SINK-0000000002
    Processor: KSTREAM-PRINTER-0000000001 (stores: [])
    Sink: KSTREAM-SINK-0000000002 (topic: streams-output)
Etat Kafka Streams : CREATED -> REBALANCING
Etat Kafka Streams : REBALANCING -> RUNNING
```

Saisissez dans le terminal 2 : `Laurel:Hardy` → terminal 1 : `[lu]: Laurel, Hardy` ; terminal 3 : `Laurel	Hardy`.
Arrêtez l'application (Ctrl+C) avant l'exercice suivant.

### B.2 Filtrage

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsFilter.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

/**
 * TP Kafka Streams 2 - filtrage : ne conserve que les messages contenant "error".
 * Usage : ./run.sh streams.StreamsFilter
 */
public class StreamsFilter {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-input");
        KStream<String, String> errors = source
                .filter((key, value) -> value != null && value.toLowerCase().contains("error"));
        errors.print(Printed.<String, String>toSysOut().withLabel("erreur"));
        errors.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-filter"));
    }
}
EOF_KAFKA_KRAFT
```

```bash
./run.sh streams.StreamsFilter
```

Saisissez `app1:ERROR disque plein`, `app2:info démarrage`, `app1:Error réseau` : seules les lignes
contenant `error` (quelle que soit la casse) arrivent dans `streams-output`.

### B.3 Transformation 1 → 1 : `mapValues`

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsMapValues.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

/**
 * TP Kafka Streams 3 - transformation 1 -> 1 : mapValues (la clé n'est pas modifiée,
 * donc pas de repartitionnement).
 * Usage : ./run.sh streams.StreamsMapValues
 */
public class StreamsMapValues {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> upper = builder.<String, String>stream("streams-input")
                .mapValues(value -> value == null ? null : value.toUpperCase());
        upper.print(Printed.<String, String>toSysOut().withLabel("majuscules"));
        upper.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-mapvalues"));
    }
}
EOF_KAFKA_KRAFT
```

```bash
./run.sh streams.StreamsMapValues
```

`k1:bonjour kafka streams` → `k1	BONJOUR KAFKA STREAMS`

### B.4 Transformation 1 → N : `flatMap`

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsFlatMap.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.ArrayList;
import java.util.List;

/**
 * TP Kafka Streams 4 - transformation 1 -> N : flatMap découpe une phrase en mots.
 * La nouvelle clé est le mot lui-même (flatMap peut changer la clé, contrairement
 * à flatMapValues).
 * Usage : ./run.sh streams.StreamsFlatMap
 */
public class StreamsFlatMap {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> words = builder.<String, String>stream("streams-input")
                .flatMap((key, value) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();
                    if (value != null) {
                        for (String word : value.toLowerCase().split("\\W+")) {
                            if (!word.isBlank()) {
                                result.add(KeyValue.pair(word, key == null ? "" : key));
                            }
                        }
                    }
                    return result;
                });
        words.print(Printed.<String, String>toSysOut().withLabel("mot"));
        words.to("streams-output");
        StreamsSupport.run(builder, StreamsSupport.config("streams-flatmap"));
    }
}
EOF_KAFKA_KRAFT
```

```bash
./run.sh streams.StreamsFlatMap
```

`phrase1:Voici un exemple de phrase découpée` → 6 messages `voici	phrase1`, `un	phrase1`, ...

> `flatMap` change la clé : si un agrégat suivait, Kafka Streams ajouterait un topic de **repartition**.
> Préférer `flatMapValues` / `mapValues` quand la clé ne change pas.

### B.5 `peek` et `foreach`

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsForeach.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * TP Kafka Streams 5 - opérations terminales : peek (observation sans modifier le flux)
 * et foreach (action finale, aucun topic de sortie).
 * Usage : ./run.sh streams.StreamsForeach
 */
public class StreamsForeach {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("streams-input")
                .peek((key, value) -> System.out.println("peek    : reçu " + key + " -> " + value))
                .filter((key, value) -> value != null && value.length() > 3)
                .foreach((key, value) -> System.out.printf("foreach : [%s] longueur=%d%n", value, value.length()));
        StreamsSupport.run(builder, StreamsSupport.config("streams-foreach"));
    }
}
EOF_KAFKA_KRAFT
```

```bash
./run.sh streams.StreamsForeach
```

```text
peek    : reçu k1 -> abc
peek    : reçu k2 -> kafka streams
foreach : [kafka streams] longueur=13
```

---

## Partie C – Traitement avec état : comptage de mots

```bash
cd ~/KAFKA-KRAFT
mkdir -p data
cat > data/words.txt <<'EOF_KAFKA_KRAFT'
Kafka est une plateforme de streaming d evenements distribuee
Un topic est decoupe en partitions repliquees sur plusieurs brokers
Les producers ecrivent les evenements dans les partitions du topic
Les consumers lisent les evenements et memorisent leur offset
Un consumer group partage les partitions entre ses membres
Kafka Streams transforme les evenements d un topic vers un autre topic
Le mode KRaft remplace ZooKeeper par un quorum de controleurs
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/WordCount.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

/**
 * TP Kafka Streams 6 - traitement AVEC ETAT : comptage de mots (KTable).
 * <ul>
 *   <li>flatMapValues + groupBy : repartitionnement par mot (topic interne -repartition)</li>
 *   <li>count() : état stocké dans RocksDB + topic interne -changelog (tolérance aux pannes)</li>
 *   <li>processing.guarantee=exactly_once_v2</li>
 * </ul>
 * Usage : ./run.sh streams.WordCount
 */
public class WordCount {

    public static void main(String[] args) {
        Properties props = StreamsSupport.config("wordcount-app");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Long> counts = builder.<String, String>stream("words-input")
                .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
                .filter((key, word) -> !word.isBlank())
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        counts.toStream()
                .peek((word, count) -> System.out.printf("%-15s %d%n", word, count))
                .to("words-output", Produced.with(Serdes.String(), Serdes.Long()));

        StreamsSupport.run(builder, props);
    }
}
EOF_KAFKA_KRAFT
```

```bash
# Terminal 1
cd ~/KAFKA-KRAFT/java/kafka-java-native && ./run.sh streams.WordCount
```

```bash
# Terminal 2 : envoi du texte, puis consultation du résultat (valeurs Long)
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic words-input < ~/KAFKA-KRAFT/data/words.txt
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic words-output --from-beginning --timeout-ms 10000 \
  --formatter-property print.key=true --value-deserializer org.apache.kafka.common.serialization.LongDeserializer | sort -k2 -n | tail -5
```

```text
topic	6
un	4
de	3
les	5
...
```

Topics **internes** créés par l'application :

```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep wordcount-app
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic wordcount-app-word-counts-changelog | head -1
ls /tmp/kafka-streams/wordcount-app/
```

```text
wordcount-app-word-counts-changelog
wordcount-app-word-counts-repartition
Topic: wordcount-app-word-counts-changelog	... PartitionCount: 3	ReplicationFactor: 3	Configs: cleanup.policy=compact,...
1_0  1_1  1_2  ...          <- un répertoire par tâche avec état (sous-topologie 1 = après repartition)
```

- Le *changelog* est **compacté** : il contient le dernier compte de chaque mot (sauvegarde de l'état)
- `/tmp/kafka-streams/wordcount-app/` : état **local** RocksDB, une sous-arborescence par **tâche**

### C.1 Scalabilité et tolérance aux pannes

Lancez une **seconde instance** dans un nouveau terminal (`./run.sh streams.WordCount`) :

```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group wordcount-app --members
```

Les 6 tâches (3 partitions d'entrée + 3 partitions de repartition) sont réparties sur les 2 instances.
Arrêtez une instance : l'autre **reprend ses tâches** et restaure l'état depuis le changelog. Envoyez de
nouveau le texte : les comptes continuent de croître (aucune perte d'état).

### C.2 Réinitialiser une application

Arrêtez toutes les instances puis :

```bash
kafka-streams-application-reset.sh --bootstrap-server $BOOTSTRAP --application-id wordcount-app --input-topics words-input
rm -rf /tmp/kafka-streams/wordcount-app
```

```text
Reset-offsets for input topics [words-input]
Following input topics offsets will be reset to (for consumer group wordcount-app)
Topic: words-input Partition: 0 Offset: 0
...
Deleting inferred internal topics [wordcount-app-word-counts-repartition, wordcount-app-word-counts-changelog]
Done.
```

Au prochain démarrage, l'application retraite tout l'historique depuis le début.

---

## Partie D – Cas métier : analyse des commandes (JSON, split, agrégat, fenêtre)

```bash
cd ~/KAFKA-KRAFT
mkdir -p java/kafka-java-native/src/main/java/fr/formation/kafka/streams
cat > java/kafka-java-native/src/main/java/fr/formation/kafka/streams/OrdersAnalytics.java <<'EOF_KAFKA_KRAFT'
package fr.formation.kafka.streams;

import fr.formation.kafka.common.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * TP Kafka Streams 7 - cas métier sur le topic "orders" (JSON) :
 * <ol>
 *   <li>désérialisation JSON et filtrage des commandes invalides (quantité <= 0)</li>
 *   <li>split() : les commandes >= 500 EUR vont dans "orders-big"</li>
 *   <li>agrégat : chiffre d'affaires cumulé par pays -> "orders-revenue-by-country"</li>
 *   <li>fenêtre : nombre de commandes par pays et par minute (fenêtres "tumbling")</li>
 * </ol>
 * Un message illisible (JSON invalide) est journalisé puis ignoré grâce au
 * "processing.exception.handler" (KIP-1033) au lieu d'arrêter l'application.
 * Usage : ./run.sh streams.OrdersAnalytics
 */
public class OrdersAnalytics {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Order> orders = builder.<String, String>stream("orders")
                .mapValues(Order::fromJson)
                .filter((customer, order) -> order.quantity() > 0);

        // 1) aiguillage des grosses commandes
        orders.split()
                .branch((customer, order) -> order.amount() >= 500,
                        Branched.withConsumer(big -> big
                                .peek((c, o) -> System.out.printf("GROSSE COMMANDE %s %.2f EUR%n", o.orderId(), o.amount()))
                                .mapValues(Order::toJson)
                                .to("orders-big")))
                .noDefaultBranch();

        // 2) chiffre d'affaires par pays (re-clé par pays => repartitionnement)
        orders.map((customer, order) -> KeyValue.pair(order.country(), order.amount()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((country, total) -> System.out.printf("CA cumulé %s = %.2f EUR%n", country, total))
                .to("orders-revenue-by-country", Produced.with(Serdes.String(), Serdes.Double()));

        // 3) nombre de commandes par pays sur des fenêtres d'1 minute
        orders.map((customer, order) -> KeyValue.pair(order.country(), order.orderId()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count()
                .toStream()
                .foreach((window, count) -> System.out.printf("fenêtre [%s - %s] %s : %d commande(s)%n",
                        window.window().startTime(), window.window().endTime(), window.key(), count));

        Properties props = StreamsSupport.config("orders-analytics");
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueProcessingExceptionHandler.class.getName());
        StreamsSupport.run(builder, props);
    }
}
EOF_KAFKA_KRAFT
```

```bash
# Terminal 1
cd ~/KAFKA-KRAFT/java/kafka-java-native && ./run.sh streams.OrdersAnalytics
```

L'application relit l'historique de `orders` (TP03) puis traite les nouvelles commandes :

```bash
# Terminal 2
cd ~/KAFKA-KRAFT/java/kafka-java-native && ./run.sh producer.OrderProducer orders 200
```

```text
GROSSE COMMANDE ORD-0012 1299.00 EUR
CA cumulé FR = 15234.70 EUR
CA cumulé BE = 3120.40 EUR
fenêtre [2026-11-09T10:21:00Z - 2026-11-09T10:22:00Z] FR : 11 commande(s)
...
```

```bash
# Terminal 3 : contenu des topics de sortie
source ~/KAFKA-KRAFT/scripts/kafka-env.sh
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic orders-revenue-by-country --from-beginning --timeout-ms 8000 \
  --formatter-property print.key=true --value-deserializer org.apache.kafka.common.serialization.DoubleDeserializer | sort | uniq | tail -8
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic orders-big --from-beginning --timeout-ms 5000 | head -3
```

> Les commandes de quantité 0 (`ORD-0017`, `ORD-0042`) sont filtrées. Un message JSON invalide serait
> journalisé puis **ignoré** (`processing.exception.handler=LogAndContinueProcessingExceptionHandler`) au lieu
> d'arrêter l'application. Testez : `echo "pas du json" | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic orders`.

---

## Partie E – Kafka Streams avec Spring for Apache Kafka

La classe `OrdersStreamsConfig` du projet Spring (TP03, Partie C) déclare un bean `KStream` :
Spring Boot crée le `StreamsBuilder`, démarre et arrête `KafkaStreams` avec le contexte applicatif.

```bash
cd ~/KAFKA-KRAFT/java/spring-kafka-demo
mvn -q spring-boot:run -Dspring-boot.run.arguments=--app.streams.enabled=true
```

Dans un autre terminal :

```bash
curl -s -X POST localhost:8085/api/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"C-003","product":"Laptop","quantity":1,"unitPrice":1299}' | jq -c
```

Logs :

```text
[Streams] grosse commande WEB-... : 1299.0 EUR
```

```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group spring-orders-streams
curl -s localhost:8085/actuator/prometheus | grep -m3 '^kafka_stream'
```

> Spring apporte : configuration par `application.yml` (`spring.kafka.streams.*`), cycle de vie,
> métriques Micrometer, `StreamsBuilderFactoryBean` (accès à l'objet `KafkaStreams`), sérialiseurs JSON.

Arrêtez l'application (Ctrl+C).

---

## Ce qu'il faut retenir

1. Une application Kafka Streams est une **simple application Java** : pas de cluster de traitement à opérer.
2. `application.id` = identité de l'application (groupe, topics internes, répertoire d'état).
3. Changement de clé + agrégat ⇒ **repartition** ; agrégat ⇒ **state store + changelog**.
4. Scalabilité = nombre de partitions d'entrée ; tolérance aux pannes = changelog + rééquilibrage.
5. `exactly_once_v2` pour des agrégats exacts ; gestionnaires d'exceptions pour les messages invalides.

### Questions de révision

1. Pourquoi `count()` après `groupBy()` crée-t-il un topic `-repartition` ?
2. Que contient le topic `-changelog` et pourquoi est-il compacté ?
3. Combien d'instances actives au maximum pour un topic d'entrée à 3 partitions ?

✅ **Suite : TP07c – Schema Registry.**
