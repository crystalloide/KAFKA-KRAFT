# `StreamsFilter.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsFilter.java` · 22 lignes

Exemple de filtrage Kafka Streams : seuls les messages contenant « error » sont recopiés dans le topic de sortie.

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

## Lignes 7 à 11 · Documentation et déclaration de la classe

```java
 7  /**
 8   * TP Kafka Streams 2 - filtrage : ne conserve que les messages contenant "error".
 9   * Usage : ./run.sh streams.StreamsFilter
10   */
11  public class StreamsFilter {
```

- **L7–10** — Javadoc : filtrage des messages contenant « error » ; lancement par `./run.sh streams.StreamsFilter`.
- **L11** — Déclare la classe publique `StreamsFilter`.

## Lignes 13 à 22 · La topologie

```java
13      public static void main(String[] args) {
14          StreamsBuilder builder = new StreamsBuilder();
15          KStream<String, String> source = builder.stream("streams-input");
16          KStream<String, String> errors = source
17                  .filter((key, value) -> value != null && value.toLowerCase().contains("error"));
18          errors.print(Printed.<String, String>toSysOut().withLabel("erreur"));
19          errors.to("streams-output");
20          StreamsSupport.run(builder, StreamsSupport.config("streams-filter"));
21      }
22  }
```

- **L13** — Méthode `main`.
- **L14** — Crée le `StreamsBuilder`.
- **L15** — Source : flux lisant le topic `streams-input`.
- **L16** — Déclare un nouveau flux `errors`, obtenu à partir de la source…
- **L17** — …par `filter` : le prédicat reçoit la clé et la valeur et ne laisse passer que les enregistrements dont la valeur n'est pas nulle et contient « error », quelle que soit la casse. C'est une opération sans état.
- **L18** — Affiche les enregistrements retenus avec l'étiquette « erreur ».
- **L19** — Écrit les enregistrements retenus dans `streams-output`, avec les serdes par défaut.
- **L20** — Configure l'application (`streams-filter`), la démarre et attend son arrêt.
- **L21** — Ferme le bloc ouvert ligne 13.
- **L22** — Ferme le bloc ouvert ligne 11.
