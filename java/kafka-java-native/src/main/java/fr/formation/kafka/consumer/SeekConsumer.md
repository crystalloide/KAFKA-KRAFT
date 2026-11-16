# `SeekConsumer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/consumer/SeekConsumer.java` · 68 lignes

Consommateur de relecture du TP03 : sans groupe de consommateurs, il s'attribue lui-même toutes les partitions, se positionne sur l'offset correspondant à une date, relit les messages puis s'arrête.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 18 · Package et imports

```java
 1  package fr.formation.kafka.consumer;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.consumer.ConsumerConfig;
 5  import org.apache.kafka.clients.consumer.ConsumerRecord;
 6  import org.apache.kafka.clients.consumer.KafkaConsumer;
 7  import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
 8  import org.apache.kafka.common.PartitionInfo;
 9  import org.apache.kafka.common.TopicPartition;
10  import org.apache.kafka.common.serialization.StringDeserializer;
11  
12  import java.time.Duration;
13  import java.time.Instant;
14  import java.util.List;
15  import java.util.Map;
16  import java.util.Properties;
17  import java.util.function.Function;
18  import java.util.stream.Collectors;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.consumer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `ConsumerConfig` : constantes des noms de paramètres du consommateur.
- **L5** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L6** — Importe `KafkaConsumer` : le client consommateur Kafka (non thread-safe : un consommateur par thread).
- **L7** — Importe `OffsetAndTimestamp` : couple offset / horodatage rendu par la recherche d'offset par date.
- **L8** — Importe `PartitionInfo` : informations sur une partition d'un topic (numéro, leader, réplicas).
- **L9** — Importe `TopicPartition` : identifie une partition : couple (topic, numéro de partition).
- **L10** — Importe `StringDeserializer` : désérialiseur qui convertit des octets (UTF-8) en `String`.
- **L12** — Importe `Duration` : durée (utilisée pour les délais de `poll()` et les fenêtres).
- **L13** — Importe `Instant` : instant sur la ligne du temps (horodatage).
- **L14** — Importe `List` : interface des listes.
- **L15** — Importe `Map` : interface des tables clé → valeur.
- **L16** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.
- **L17** — Importe `Function` : interface fonctionnelle : une fonction d'un argument.
- **L18** — Importe `Collectors` : collecteurs pour terminer un flux Java (`toMap`, `toList`…).

## Lignes 20 à 25 · Documentation et déclaration de la classe

```java
20  /**
21   * TP03 - Relecture : assign() manuel (sans consumer group) et seek() par date.
22   * Relit tous les messages publiés depuis N minutes, puis s'arrête.
23   * Usage : ./run.sh consumer.SeekConsumer [topic] [minutes]
24   */
25  public class SeekConsumer {
```

- **L20–24** — Javadoc : relecture par attribution manuelle (`assign`, sans groupe) et positionnement par date (`seek`). Le programme relit les messages publiés depuis N minutes puis s'arrête. Lancement : `./run.sh consumer.SeekConsumer [topic] [minutes]`.
- **L25** — Déclare la classe publique `SeekConsumer`.

## Lignes 27 à 29 · Point d'entrée et paramètres

```java
27      public static void main(String[] args) {
28          String topic = args.length > 0 ? args[0] : "test-formation";
29          long minutes = args.length > 1 ? Long.parseLong(args[1]) : 10;
```

- **L27** — Méthode `main`.
- **L28** — Topic à relire : premier argument, ou `test-formation`.
- **L29** — Profondeur de relecture en minutes : second argument, ou 10.

## Lignes 31 à 34 · Configuration du consommateur

```java
31          Properties props = KafkaSettings.base("seek-consumer");
32          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
33          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
34          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

- **L31** — Propriétés de base avec `client.id=seek-consumer`. Aucun `group.id` n'est défini.
- **L32** — Désérialiseur de la clé.
- **L33** — Désérialiseur de la valeur.
- **L34** — `enable.auto.commit=false` : sans groupe, il n'y a aucun offset à valider.

## Lignes 36 à 40 · Attribution manuelle des partitions

```java
36          try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
37              List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
38                      .map((PartitionInfo p) -> new TopicPartition(topic, p.partition()))
39                      .toList();
40              consumer.assign(partitions);   // pas de group.id : aucun rééquilibrage, aucun commit
```

- **L36** — Crée le consommateur, fermé automatiquement en fin de bloc.
- **L37** — Demande au cluster la liste des partitions du topic et la transforme en flux Java.
- **L38** — Convertit chaque `PartitionInfo` en `TopicPartition` (topic + numéro).
- **L39** — Rassemble le tout dans une liste.
- **L40** — `assign` attribue directement ces partitions au consommateur. Contrairement à `subscribe`, il n'y a ni groupe, ni rééquilibrage, ni validation d'offsets.

## Lignes 42 à 46 · Recherche des offsets par date

```java
42              long since = Instant.now().minus(Duration.ofMinutes(minutes)).toEpochMilli();
43              Map<TopicPartition, Long> query = partitions.stream()
44                      .collect(Collectors.toMap(Function.identity(), tp -> since));
45              Map<TopicPartition, OffsetAndTimestamp> found = consumer.offsetsForTimes(query);
46              Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
```

- **L42** — Calcule l'horodatage de départ : maintenant moins N minutes, en millisecondes depuis 1970.
- **L43–44** — Construit la requête : une table associant à chaque partition l'horodatage cherché.
- **L45** — Interroge les brokers : pour chaque partition, quel est le premier offset dont l'horodatage est supérieur ou égal à la date demandée ?
- **L46** — Récupère aussi l'offset de fin de chaque partition (celui du prochain message à écrire).

## Lignes 48 à 53 · Positionnement sur chaque partition

```java
48              for (TopicPartition tp : partitions) {
49                  OffsetAndTimestamp oat = found.get(tp);
50                  long start = (oat != null) ? oat.offset() : endOffsets.get(tp);
51                  consumer.seek(tp, start);
52                  System.out.printf("%s : relecture à partir de l'offset %d (fin = %d)%n", tp, start, endOffsets.get(tp));
53              }
```

- **L48** — Parcourt les partitions.
- **L49** — Lit le résultat de la recherche ; il vaut `null` s'il n'existe aucun message aussi récent dans la partition.
- **L50** — Offset de départ : celui trouvé, ou à défaut la fin de la partition (rien à relire).
- **L51** — `seek` positionne le consommateur sur cet offset pour cette partition.
- **L52** — Affiche le point de départ de la relecture et l'offset de fin.
- **L53** — Ferme le bloc ouvert ligne 48.

## Lignes 55 à 68 · Relecture jusqu'à épuisement

```java
55              int empty = 0;
56              while (empty < 3) {
57                  var records = consumer.poll(Duration.ofMillis(1000));
58                  if (records.isEmpty()) {
59                      empty++;
60                  }
61                  for (ConsumerRecord<String, String> r : records) {
62                      System.out.printf("%s partition=%d offset=%d value=%s%n",
63                              Instant.ofEpochMilli(r.timestamp()), r.partition(), r.offset(), r.value());
64                  }
65              }
66          }
67      }
68  }
```

- **L55** — Compteur de lots vides. Il n'est pas remis à zéro quand un lot non vide arrive : ce sont des lots vides cumulés, pas consécutifs.
- **L56** — Continue tant qu'on n'a pas obtenu trois lots vides : c'est le critère d'arrêt, simple, d'un programme de démonstration.
- **L57** — Récupère un lot (attente d'une seconde au plus). `var` laisse le compilateur déduire le type `ConsumerRecords<String, String>`.
- **L58** — Si le lot est vide…
- **L59** — …incrémente le compteur.
- **L60** — Ferme le bloc ouvert ligne 58.
- **L61** — Parcourt les messages du lot.
- **L62–63** — Affiche la date du message (son horodatage converti en `Instant`), la partition, l'offset et la valeur.
- **L64** — Ferme le bloc ouvert ligne 61.
- **L65** — Ferme le bloc ouvert ligne 56.
- **L66** — Ferme le bloc ouvert ligne 36.
- **L67** — Ferme le bloc ouvert ligne 27.
- **L68** — Ferme le bloc ouvert ligne 25.
