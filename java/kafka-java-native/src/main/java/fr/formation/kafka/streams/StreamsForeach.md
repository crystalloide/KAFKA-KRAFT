# `StreamsForeach.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/StreamsForeach.java` · 20 lignes

Exemple des opérations d'observation : `peek` regarde passer les enregistrements sans modifier le flux, `foreach` termine le flux par une action, sans topic de sortie.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 3 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import org.apache.kafka.streams.StreamsBuilder;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.

## Lignes 5 à 10 · Documentation et déclaration de la classe

```java
 5  /**
 6   * TP Kafka Streams 5 - opérations terminales : peek (observation sans modifier le flux)
 7   * et foreach (action finale, aucun topic de sortie).
 8   * Usage : ./run.sh streams.StreamsForeach
 9   */
10  public class StreamsForeach {
```

- **L5–9** — Javadoc : `peek` (observation sans modifier le flux) et `foreach` (action finale, aucun topic de sortie) ; lancement par `./run.sh streams.StreamsForeach`.
- **L10** — Déclare la classe publique `StreamsForeach`.

## Lignes 12 à 20 · La topologie

```java
12      public static void main(String[] args) {
13          StreamsBuilder builder = new StreamsBuilder();
14          builder.<String, String>stream("streams-input")
15                  .peek((key, value) -> System.out.println("peek    : reçu " + key + " -> " + value))
16                  .filter((key, value) -> value != null && value.length() > 3)
17                  .foreach((key, value) -> System.out.printf("foreach : [%s] longueur=%d%n", value, value.length()));
18          StreamsSupport.run(builder, StreamsSupport.config("streams-foreach"));
19      }
20  }
```

- **L12** — Méthode `main`.
- **L13** — Crée le `StreamsBuilder`.
- **L14** — Lit le topic `streams-input`. Le flux n'est pas stocké dans une variable : les opérations sont chaînées directement.
- **L15** — `peek` : affiche chaque enregistrement reçu et le transmet tel quel à l'étape suivante.
- **L16** — `filter` : ne garde que les valeurs non nulles de plus de 3 caractères.
- **L17** — `foreach` : opération terminale ; affiche la valeur et sa longueur. Elle ne renvoie pas de flux : rien ne peut être chaîné derrière et rien n'est écrit dans Kafka.
- **L18** — Configure l'application (`streams-foreach`), la démarre et attend son arrêt.
- **L19** — Ferme le bloc ouvert ligne 12.
- **L20** — Ferme le bloc ouvert ligne 10.
