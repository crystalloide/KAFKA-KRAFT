# `StreamsFlatMap.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsFlatMap.java` · 37 lignes

Exemple de transformation un pour plusieurs : chaque phrase reçue est découpée en mots, et chaque mot devient un nouvel enregistrement dont il est la clé.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 9 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import org.apache.kafka.streams.KeyValue;
 4  import org.apache.kafka.streams.StreamsBuilder;
 5  import org.apache.kafka.streams.kstream.KStream;
 6  import org.apache.kafka.streams.kstream.Printed;
 7  
 8  import java.util.ArrayList;
 9  import java.util.List;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KeyValue` : couple clé / valeur manipulé par les opérations du DSL.
- **L4** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L5** — Importe `KStream` : flux d'enregistrements : chaque message est un fait indépendant.
- **L6** — Importe `Printed` : décrit un affichage du flux (ici sur la sortie standard) pour la mise au point.
- **L8** — Importe `ArrayList` : liste modifiable.
- **L9** — Importe `List` : interface des listes.

## Lignes 11 à 17 · Documentation et déclaration de la classe

```java
11  /**
12   * TP Kafka Streams 4 - transformation 1 -> N : flatMap découpe une phrase en mots.
13   * La nouvelle clé est le mot lui-même (flatMap peut changer la clé, contrairement
14   * à flatMapValues).
15   * Usage : ./run.sh streams.StreamsFlatMap
16   */
17  public class StreamsFlatMap {
```

- **L11–16** — Javadoc : `flatMap` découpe une phrase en mots ; la nouvelle clé est le mot lui-même, car `flatMap` peut changer la clé, contrairement à `flatMapValues`. Lancement par `./run.sh streams.StreamsFlatMap`.
- **L17** — Déclare la classe publique `StreamsFlatMap`.

## Lignes 19 à 32 · La topologie : découpage en mots

```java
19      public static void main(String[] args) {
20          StreamsBuilder builder = new StreamsBuilder();
21          KStream<String, String> words = builder.<String, String>stream("streams-input")
22                  .flatMap((key, value) -> {
23                      List<KeyValue<String, String>> result = new ArrayList<>();
24                      if (value != null) {
25                          for (String word : value.toLowerCase().split("\\W+")) {
26                              if (!word.isBlank()) {
27                                  result.add(KeyValue.pair(word, key == null ? "" : key));
28                              }
29                          }
30                      }
31                      return result;
32                  });
```

- **L19** — Méthode `main`.
- **L20** — Crée le `StreamsBuilder`.
- **L21** — Lit le topic `streams-input`…
- **L22** — …et applique `flatMap` : pour un enregistrement en entrée, la lambda renvoie une liste de zéro, un ou plusieurs enregistrements.
- **L23** — Crée la liste des couples clé / valeur à produire.
- **L24** — Ne traite que les valeurs non nulles.
- **L25** — Met la phrase en minuscules et la découpe sur les suites de caractères qui ne sont ni lettres, ni chiffres, ni tiret bas (expression régulière `\W+`).
- **L26** — Ignore les éléments vides (cas d'une phrase commençant par un séparateur).
- **L27** — Ajoute un enregistrement dont la clé est le mot et la valeur l'ancienne clé (ou une chaîne vide si elle était nulle).
- **L28** — Ferme le bloc ouvert ligne 26.
- **L29** — Ferme le bloc ouvert ligne 25.
- **L30** — Ferme le bloc ouvert ligne 24.
- **L31** — Renvoie la liste : Kafka Streams émet un enregistrement par élément.
- **L32** — Ferme le bloc ouvert ligne 22.

## Lignes 33 à 37 · Sortie et démarrage

```java
33          words.print(Printed.<String, String>toSysOut().withLabel("mot"));
34          words.to("streams-output");
35          StreamsSupport.run(builder, StreamsSupport.config("streams-flatmap"));
36      }
37  }
```

- **L33** — Affiche chaque enregistrement produit avec l'étiquette « mot ».
- **L34** — Écrit le résultat dans `streams-output`.
- **L35** — Configure l'application (`streams-flatmap`), la démarre et attend son arrêt.
- **L36** — Ferme le bloc ouvert ligne 19.
- **L37** — Ferme le bloc ouvert ligne 17.
