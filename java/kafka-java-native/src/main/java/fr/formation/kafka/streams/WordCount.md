# `WordCount.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/WordCount.java` · 46 lignes

Exemple de traitement avec état : le comptage de mots. Le résultat est une table (`KTable`) tenue à jour en continu et sauvegardée dans Kafka.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 14 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import org.apache.kafka.common.serialization.Serdes;
 4  import org.apache.kafka.common.utils.Bytes;
 5  import org.apache.kafka.streams.StreamsBuilder;
 6  import org.apache.kafka.streams.StreamsConfig;
 7  import org.apache.kafka.streams.kstream.Grouped;
 8  import org.apache.kafka.streams.kstream.KTable;
 9  import org.apache.kafka.streams.kstream.Materialized;
10  import org.apache.kafka.streams.kstream.Produced;
11  import org.apache.kafka.streams.state.KeyValueStore;
12  
13  import java.util.Arrays;
14  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Serdes` : fabrique de *serdes* (sérialiseur + désérialiseur) pour les types courants.
- **L4** — Importe `Bytes` : enveloppe d'un tableau d'octets, utilisée comme type de clé des *state stores*.
- **L5** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L6** — Importe `StreamsConfig` : constantes des noms de paramètres de Kafka Streams.
- **L7** — Importe `Grouped` : précise les serdes à utiliser lors d'un regroupement (et du topic de repartition éventuel).
- **L8** — Importe `KTable` : table : dernière valeur connue pour chaque clé.
- **L9** — Importe `Materialized` : précise comment matérialiser un état (nom du *state store*, serdes).
- **L10** — Importe `Produced` : précise comment écrire dans un topic de sortie (serdes de clé et de valeur).
- **L11** — Importe `KeyValueStore` : type de *state store* clé / valeur (RocksDB par défaut).
- **L13** — Importe `Arrays` : utilitaires sur les tableaux.
- **L14** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 16 à 25 · Documentation et déclaration de la classe

```java
16  /**
17   * TP Kafka Streams 6 - traitement AVEC ETAT : comptage de mots (KTable).
18   * <ul>
19   *   <li>flatMapValues + groupBy : repartitionnement par mot (topic interne -repartition)</li>
20   *   <li>count() : état stocké dans RocksDB + topic interne -changelog (tolérance aux pannes)</li>
21   *   <li>processing.guarantee=exactly_once_v2</li>
22   * </ul>
23   * Usage : ./run.sh streams.WordCount
24   */
25  public class WordCount {
```

- **L16–24** — Javadoc : traitement avec état. `flatMapValues` + `groupBy` entraînent un repartitionnement par mot (topic interne `-repartition`) ; `count()` stocke l'état dans RocksDB et dans un topic interne `-changelog` (tolérance aux pannes) ; la garantie de traitement est `exactly_once_v2`. Lancement par `./run.sh streams.WordCount`.
- **L25** — Déclare la classe publique `WordCount`.

## Lignes 27 à 29 · Configuration

```java
27      public static void main(String[] args) {
28          Properties props = StreamsSupport.config("wordcount-app");
29          props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
```

- **L27** — Méthode `main`.
- **L28** — Récupère la configuration commune avec l'identifiant d'application `wordcount-app`.
- **L29** — `processing.guarantee=exactly_once_v2` : lecture, mise à jour de l'état et écriture sont validées dans une même transaction Kafka (la valeur par défaut est `at_least_once`).

## Lignes 31 à 38 · La topologie : du texte aux compteurs

```java
31          StreamsBuilder builder = new StreamsBuilder();
32          KTable<String, Long> counts = builder.<String, String>stream("words-input")
33                  .flatMapValues(line -> Arrays.asList(line.toLowerCase().split("\\W+")))
34                  .filter((key, word) -> !word.isBlank())
35                  .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
36                  .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-counts")
37                          .withKeySerde(Serdes.String())
38                          .withValueSerde(Serdes.Long()));
```

- **L31** — Crée le `StreamsBuilder`.
- **L32** — Déclare la table résultat `counts` (mot → nombre d'occurrences), construite à partir du flux lisant `words-input`.
- **L33** — `flatMapValues` : chaque ligne est mise en minuscules et découpée en mots ; un enregistrement par mot, la clé restant inchangée.
- **L34** — `filter` : écarte les mots vides.
- **L35** — `groupBy` : regroupe par mot, qui devient la nouvelle clé. Comme la clé change, Kafka Streams passe par un topic interne de repartition, afin que toutes les occurrences d'un mot arrivent dans la même partition ; `Grouped.with` fournit les serdes de ce topic.
- **L36** — `count` : compte les enregistrements par clé. `Materialized.as("word-counts")` nomme le *state store* ; les trois types génériques sont ceux de la clé, de la valeur et du store.
- **L37** — Serde de la clé du store : `String`.
- **L38** — Serde de la valeur du store : `Long`. Chaque mise à jour du store est aussi écrite dans un topic changelog, qui permet de reconstruire l'état après une panne.

## Lignes 40 à 46 · Sortie et démarrage

```java
40          counts.toStream()
41                  .peek((word, count) -> System.out.printf("%-15s %d%n", word, count))
42                  .to("words-output", Produced.with(Serdes.String(), Serdes.Long()));
43  
44          StreamsSupport.run(builder, props);
45      }
46  }
```

- **L40** — Convertit la table en flux de ses mises à jour : un enregistrement à chaque changement d'un compteur.
- **L41** — `peek` : affiche le mot (cadré sur 15 caractères) et son compteur.
- **L42** — Écrit les mises à jour dans `words-output`. La valeur étant un `Long`, les serdes sont précisés avec `Produced.with` (pour lire ce topic en console, il faut un désérialiseur `Long`).
- **L44** — Démarre l'application avec la configuration préparée et attend son arrêt.
- **L45** — Ferme le bloc ouvert ligne 27.
- **L46** — Ferme le bloc ouvert ligne 25.
