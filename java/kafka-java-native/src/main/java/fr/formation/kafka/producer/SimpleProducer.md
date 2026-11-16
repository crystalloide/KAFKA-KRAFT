# `SimpleProducer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/producer/SimpleProducer.java` · 67 lignes

Premier producteur du TP03 : il montre les trois façons d'envoyer un message — sans attendre de réponse, de façon synchrone, de façon asynchrone avec fonction de rappel.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 11 · Package et imports

```java
 1  package fr.formation.kafka.producer;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.producer.KafkaProducer;
 5  import org.apache.kafka.clients.producer.ProducerConfig;
 6  import org.apache.kafka.clients.producer.ProducerRecord;
 7  import org.apache.kafka.clients.producer.RecordMetadata;
 8  import org.apache.kafka.common.serialization.StringSerializer;
 9  
10  import java.util.Properties;
11  import java.util.concurrent.ExecutionException;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L5** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L6** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L7** — Importe `RecordMetadata` : accusé de réception d'un envoi : topic, partition, offset, horodatage.
- **L8** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L10** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.
- **L11** — Importe `ExecutionException` : exception enveloppant l'échec d'une tâche asynchrone (levée par `Future.get()`).

## Lignes 13 à 22 · Documentation et déclaration de la classe

```java
13  /**
14   * TP03 - Premier producer : les 3 façons d'envoyer un message.
15   * <ol>
16   *   <li>"fire and forget" : send() sans attendre de réponse</li>
17   *   <li>synchrone        : send().get() bloque jusqu'à l'acquittement du broker</li>
18   *   <li>asynchrone       : send(record, callback) - recommandé en production</li>
19   * </ol>
20   * Usage : ./run.sh producer.SimpleProducer [topic] [nombre]
21   */
22  public class SimpleProducer {
```

- **L13–21** — Javadoc : les trois modes d'envoi (« fire and forget », synchrone avec `send().get()`, asynchrone avec callback — recommandé en production) et la commande de lancement avec ses arguments optionnels.
- **L22** — Déclare la classe publique `SimpleProducer`.

## Lignes 24 à 26 · Point d'entrée et paramètres

```java
24      public static void main(String[] args) throws ExecutionException, InterruptedException {
25          String topic = args.length > 0 ? args[0] : "test-formation";
26          int count = args.length > 1 ? Integer.parseInt(args[1]) : 5;
```

- **L24** — Méthode `main`. Elle déclare `ExecutionException` et `InterruptedException`, que peut lever l'appel bloquant `get()` de l'envoi synchrone.
- **L25** — Topic cible : premier argument, ou `test-formation` par défaut.
- **L26** — Nombre de messages par mode d'envoi : second argument converti en entier, ou 5 par défaut.

## Lignes 28 à 33 · Configuration du producteur

```java
28          Properties props = KafkaSettings.base("simple-producer");
29          // Un producer SERIALISE la clé et la valeur en tableaux d'octets
30          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
31          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
32          // Valeur par défaut depuis Kafka 3.0 : acquittement par tous les réplicas in-sync
33          props.put(ProducerConfig.ACKS_CONFIG, "all");
```

- **L28** — Récupère les propriétés de base (adresse du cluster, `client.id=simple-producer`).
- **L29** — Commentaire : un producteur sérialise la clé et la valeur en tableaux d'octets.
- **L30** — `key.serializer` : la clé, de type `String`, sera convertie en octets par `StringSerializer`.
- **L31** — `value.serializer` : même chose pour la valeur.
- **L32** — Commentaire : `acks=all` est la valeur par défaut depuis Kafka 3.0.
- **L33** — `acks=all` : le leader n'acquitte l'envoi qu'une fois le message répliqué sur tous les réplicas synchronisés. La valeur est écrite explicitement dans un but pédagogique.

## Lignes 35 à 36 · Création du producteur

```java
35          // try-with-resources : close() vide le buffer (flush) puis libère les connexions
36          try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
```

- **L35** — Commentaire : avec le *try-with-resources*, `close()` est appelé automatiquement ; il vide le tampon d'envoi puis libère les connexions.
- **L36** — Crée le producteur, typé `<String, String>` (type de la clé, type de la valeur). Il sera fermé à la sortie du bloc.

## Lignes 38 à 42 · Mode 1 : « fire and forget »

```java
38              System.out.println("=== 1. Fire and forget ===");
39              for (int i = 0; i < count; i++) {
40                  producer.send(new ProducerRecord<>(topic, "cle-" + (i % 3), "fire-and-forget " + i));
41              }
42              producer.flush();
```

- **L38** — Affiche le titre de la première partie.
- **L39** — Boucle sur le nombre de messages demandé.
- **L40** — Crée un message (topic, clé `cle-0` / `cle-1` / `cle-2` selon `i % 3`, valeur) et l'envoie sans exploiter le résultat. `send()` ne fait que déposer le message dans un tampon : une erreur d'envoi passerait ici inaperçue.
- **L41** — Ferme le bloc ouvert ligne 39.
- **L42** — `flush()` force l'envoi immédiat de tout ce qui est en tampon et attend la fin des envois, pour bien séparer les trois parties de la démonstration.

## Lignes 44 à 49 · Mode 2 : envoi synchrone

```java
44              System.out.println("=== 2. Envoi synchrone (send().get()) ===");
45              for (int i = 0; i < count; i++) {
46                  RecordMetadata md = producer.send(
47                          new ProducerRecord<>(topic, "cle-" + (i % 3), "synchrone " + i)).get();
48                  System.out.printf("synchrone %d -> partition %d, offset %d%n", i, md.partition(), md.offset());
49              }
```

- **L44** — Affiche le titre de la deuxième partie.
- **L45** — Boucle sur le nombre de messages.
- **L46–47** — Envoie le message puis appelle `get()` sur le `Future` renvoyé : le programme est bloqué jusqu'à l'acquittement du broker, et récupère le `RecordMetadata`. Simple, mais lent : un aller-retour réseau par message.
- **L48** — Affiche la partition et l'offset attribués par le broker.
- **L49** — Ferme le bloc ouvert ligne 45.

## Lignes 51 à 64 · Mode 3 : envoi asynchrone avec callback

```java
51              System.out.println("=== 3. Envoi asynchrone avec callback ===");
52              for (int i = 0; i < count; i++) {
53                  final int n = i;
54                  producer.send(new ProducerRecord<>(topic, "cle-" + (i % 3), "asynchrone " + i),
55                          (metadata, exception) -> {
56                              if (exception != null) {
57                                  System.err.printf("ECHEC message %d : %s%n", n, exception.getMessage());
58                              } else {
59                                  System.out.printf("asynchrone %d -> partition %d, offset %d%n",
60                                          n, metadata.partition(), metadata.offset());
61                              }
62                          });
63              }
64          }
```

- **L51** — Affiche le titre de la troisième partie.
- **L52** — Boucle sur le nombre de messages.
- **L53** — Copie `i` dans une variable `final` : une lambda ne peut utiliser que des variables locales qui ne changent pas.
- **L54** — Envoie le message en fournissant un second argument : la fonction de rappel.
- **L55** — Début de la lambda : elle sera appelée par le thread d'envoi du producteur avec, soit les métadonnées, soit l'exception.
- **L56** — Teste si l'envoi a échoué.
- **L57** — En cas d'échec, affiche le numéro du message et la cause sur la sortie d'erreur.
- **L58** — Sinon (envoi réussi)…
- **L59–60** — …affiche le numéro du message, la partition et l'offset.
- **L61** — Ferme le bloc ouvert ligne 58.
- **L62** — Ferme le bloc ouvert ligne 55.
- **L63** — Ferme le bloc ouvert ligne 52.
- **L64** — Ferme le bloc ouvert ligne 36.

## Lignes 65 à 67 · Fin du programme

```java
65          System.out.println("Producer fermé proprement.");
66      }
67  }
```

- **L65** — Message affiché après la sortie du bloc `try` : le producteur a été fermé, donc tous les messages en attente ont été envoyés et leurs callbacks exécutés.
- **L66** — Ferme le bloc ouvert ligne 24.
- **L67** — Ferme le bloc ouvert ligne 22.
