# `StreamsMapValues.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsMapValues.java` · 22 lignes

Exemple de transformation un pour un : chaque valeur est mise en majuscules, la clé restant inchangée.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 5 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import org.apache.kafka.streams.StreamsBuilder;
 4  import org.apache.kafka.streams.kstream.KStream;
 5  import org.apache.kafka.streams.kstream.Printed;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L4** — Importe `KStream` : flux d'enregistrements : chaque message est un fait indépendant.
- **L5** — Importe `Printed` : décrit un affichage du flux (ici sur la sortie standard) pour la mise au point.

## Lignes 7 à 12 · Documentation et déclaration de la classe

```java
 7  /**
 8   * TP Kafka Streams 3 - transformation 1 -> 1 : mapValues (la clé n'est pas modifiée,
 9   * donc pas de repartitionnement).
10   * Usage : ./run.sh streams.StreamsMapValues
11   */
12  public class StreamsMapValues {
```

- **L7–11** — Javadoc : transformation 1 → 1 avec `mapValues` ; la clé n'étant pas modifiée, il n'y a pas de repartitionnement. Lancement par `./run.sh streams.StreamsMapValues`.
- **L12** — Déclare la classe publique `StreamsMapValues`.

## Lignes 14 à 22 · La topologie

```java
14      public static void main(String[] args) {
15          StreamsBuilder builder = new StreamsBuilder();
16          KStream<String, String> upper = builder.<String, String>stream("streams-input")
17                  .mapValues(value -> value == null ? null : value.toUpperCase());
18          upper.print(Printed.<String, String>toSysOut().withLabel("majuscules"));
19          upper.to("streams-output");
20          StreamsSupport.run(builder, StreamsSupport.config("streams-mapvalues"));
21      }
22  }
```

- **L14** — Méthode `main`.
- **L15** — Crée le `StreamsBuilder`.
- **L16** — Lit le topic `streams-input` (les types `<String, String>` sont précisés pour permettre le chaînage)…
- **L17** — …et applique `mapValues` : chaque valeur est remplacée par sa version en majuscules ; une valeur nulle reste nulle, ce qui évite une `NullPointerException`.
- **L18** — Affiche le résultat avec l'étiquette « majuscules ».
- **L19** — Écrit le résultat dans `streams-output`.
- **L20** — Configure l'application (`streams-mapvalues`), la démarre et attend son arrêt.
- **L21** — Ferme le bloc ouvert ligne 14.
- **L22** — Ferme le bloc ouvert ligne 12.
