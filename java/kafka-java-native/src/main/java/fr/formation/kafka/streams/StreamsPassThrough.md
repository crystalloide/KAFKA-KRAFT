# `StreamsPassThrough.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsPassThrough.java` · 22 lignes

Premier exemple Kafka Streams : copier un topic vers un autre sans transformation, en affichant chaque message au passage.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 7 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import org.apache.kafka.common.serialization.Serdes;
 4  import org.apache.kafka.streams.StreamsBuilder;
 5  import org.apache.kafka.streams.kstream.KStream;
 6  import org.apache.kafka.streams.kstream.Printed;
 7  import org.apache.kafka.streams.kstream.Produced;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Serdes` : fabrique de *serdes* (sérialiseur + désérialiseur) pour les types courants.
- **L4** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L5** — Importe `KStream` : flux d'enregistrements : chaque message est un fait indépendant.
- **L6** — Importe `Printed` : décrit un affichage du flux (ici sur la sortie standard) pour la mise au point.
- **L7** — Importe `Produced` : précise comment écrire dans un topic de sortie (serdes de clé et de valeur).

## Lignes 9 à 13 · Documentation et déclaration de la classe

```java
 9  /**
10   * TP Kafka Streams 1 - copie simple d'un topic vers un autre (sans transformation).
11   * Usage : ./run.sh streams.StreamsPassThrough
12   */
13  public class StreamsPassThrough {
```

- **L9–12** — Javadoc : copie simple d'un topic vers un autre ; lancement par `./run.sh streams.StreamsPassThrough`.
- **L13** — Déclare la classe publique `StreamsPassThrough`.

## Lignes 15 à 22 · La topologie

```java
15      public static void main(String[] args) {
16          StreamsBuilder builder = new StreamsBuilder();
17          KStream<String, String> source = builder.stream("streams-input");
18          source.print(Printed.<String, String>toSysOut().withLabel("lu"));
19          source.to("streams-output", Produced.with(Serdes.String(), Serdes.String()));
20          StreamsSupport.run(builder, StreamsSupport.config("streams-passthrough"));
21      }
22  }
```

- **L15** — Méthode `main`.
- **L16** — Crée le `StreamsBuilder`, avec lequel on décrit la topologie.
- **L17** — Déclare la source : un `KStream` qui lit le topic `streams-input`, avec les serdes par défaut (`String` / `String`).
- **L18** — Ajoute un affichage de chaque enregistrement sur la sortie standard, préfixé par l'étiquette « lu ». Les types génériques sont précisés devant `toSysOut()` car le compilateur ne peut pas les déduire.
- **L19** — Ajoute le puits : chaque enregistrement est écrit dans `streams-output`. `Produced.with` indique explicitement les serdes de la clé et de la valeur (ici identiques aux serdes par défaut).
- **L20** — Construit la configuration avec l'identifiant d'application `streams-passthrough`, puis démarre l'application et attend son arrêt.
- **L21** — Ferme le bloc ouvert ligne 15.
- **L22** — Ferme le bloc ouvert ligne 13.
