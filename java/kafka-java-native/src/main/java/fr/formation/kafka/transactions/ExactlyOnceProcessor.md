# `ExactlyOnceProcessor.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/transactions/ExactlyOnceProcessor.java` · 95 lignes

Traitement « exactement une fois » du TP03 : lire `transactions-in`, mettre les valeurs en majuscules, écrire dans `transactions-out`, et valider les offsets lus dans la même transaction que les écritures.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 22 · Package et imports

```java
 1  package fr.formation.kafka.transactions;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.consumer.ConsumerConfig;
 5  import org.apache.kafka.clients.consumer.ConsumerRecord;
 6  import org.apache.kafka.clients.consumer.ConsumerRecords;
 7  import org.apache.kafka.clients.consumer.KafkaConsumer;
 8  import org.apache.kafka.clients.consumer.OffsetAndMetadata;
 9  import org.apache.kafka.clients.producer.KafkaProducer;
10  import org.apache.kafka.clients.producer.ProducerConfig;
11  import org.apache.kafka.clients.producer.ProducerRecord;
12  import org.apache.kafka.common.KafkaException;
13  import org.apache.kafka.common.TopicPartition;
14  import org.apache.kafka.common.errors.WakeupException;
15  import org.apache.kafka.common.serialization.StringDeserializer;
16  import org.apache.kafka.common.serialization.StringSerializer;
17  
18  import java.time.Duration;
19  import java.util.HashMap;
20  import java.util.List;
21  import java.util.Map;
22  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.transactions`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `ConsumerConfig` : constantes des noms de paramètres du consommateur.
- **L5** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L6** — Importe `ConsumerRecords` : le lot de messages rendu par un appel à `poll()`.
- **L7** — Importe `KafkaConsumer` : le client consommateur Kafka (non thread-safe : un consommateur par thread).
- **L8** — Importe `OffsetAndMetadata` : offset à valider (commit) pour une partition, avec une métadonnée optionnelle.
- **L9** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L10** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L11** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L12** — Importe `KafkaException` : classe mère des exceptions Kafka (non vérifiées).
- **L13** — Importe `TopicPartition` : identifie une partition : couple (topic, numéro de partition).
- **L14** — Importe `WakeupException` : exception levée dans `poll()` quand un autre thread a appelé `consumer.wakeup()`.
- **L15** — Importe `StringDeserializer` : désérialiseur qui convertit des octets (UTF-8) en `String`.
- **L16** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L18** — Importe `Duration` : durée (utilisée pour les délais de `poll()` et les fenêtres).
- **L19** — Importe `HashMap` : table de hachage clé → valeur.
- **L20** — Importe `List` : interface des listes.
- **L21** — Importe `Map` : interface des tables clé → valeur.
- **L22** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 24 à 33 · Documentation et déclaration de la classe

```java
24  /**
25   * TP03 - Traitement "exactly-once" : consume -> transform -> produce.
26   * <p>
27   * Les offsets consommés sont commités DANS la transaction du producer
28   * (sendOffsetsToTransaction) : lecture, écriture et commit sont atomiques.
29   * Le consumer lit en isolation.level=read_committed : il ignore les messages
30   * des transactions annulées.
31   * Usage : ./run.sh transactions.ExactlyOnceProcessor
32   */
33  public class ExactlyOnceProcessor {
```

- **L24–32** — Javadoc : schéma consume → transform → produce. Les offsets consommés sont validés dans la transaction du producteur (`sendOffsetsToTransaction`) : lecture, écriture et validation sont atomiques. Le consommateur lit en `read_committed` et ignore donc les messages des transactions annulées. Lancement : `./run.sh transactions.ExactlyOnceProcessor`.
- **L33** — Déclare la classe publique `ExactlyOnceProcessor`.

## Lignes 35 à 42 · Configuration du consommateur

```java
35      public static void main(String[] args) {
36          Properties cprops = KafkaSettings.base("eos-consumer");
37          cprops.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-processor");
38          cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
39          cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
40          cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
41          cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
42          cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
```

- **L35** — Méthode `main`.
- **L36** — Propriétés du consommateur, avec `client.id=eos-consumer`.
- **L37** — `group.id=eos-processor` : groupe du traitement.
- **L38** — Désérialiseur de la clé.
- **L39** — Désérialiseur de la valeur.
- **L40** — `auto.offset.reset=earliest` : lecture depuis le début en l'absence d'offset validé.
- **L41** — `enable.auto.commit=false` : indispensable, car les offsets seront validés par le producteur, dans la transaction.
- **L42** — `isolation.level=read_committed` : le consommateur ne voit que les messages de transactions validées (et les messages non transactionnels).

## Lignes 44 à 47 · Configuration du producteur

```java
44          Properties pprops = KafkaSettings.base("eos-producer");
45          pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
46          pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
47          pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "formation-eos-processor-1");
```

- **L44** — Propriétés du producteur, avec `client.id=eos-producer`.
- **L45** — Sérialiseur de la clé.
- **L46** — Sérialiseur de la valeur.
- **L47** — `transactional.id` : identifiant stable de cette instance du traitement ; il rend le producteur transactionnel.

## Lignes 49 à 59 · Création des clients et arrêt propre

```java
49          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
50          KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);
51          Thread mainThread = Thread.currentThread();
52          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
53              consumer.wakeup();
54              try {
55                  mainThread.join();
56              } catch (InterruptedException e) {
57                  Thread.currentThread().interrupt();
58              }
59          }));
```

- **L49** — Crée le consommateur.
- **L50** — Crée le producteur.
- **L51** — Mémorise le thread principal.
- **L52** — Enregistre un *shutdown hook* pour Ctrl+C.
- **L53** — Interrompt le `poll()` en cours par une `WakeupException`.
- **L54** — Ouvre un bloc `try`.
- **L55** — Attend la fin du thread principal, donc la fermeture propre des deux clients.
- **L56** — Intercepte l'interruption éventuelle.
- **L57** — Rétablit l'indicateur d'interruption.
- **L58** — Ferme le bloc ouvert ligne 56.
- **L59** — Ferme le bloc ouvert ligne 52.

## Lignes 61 à 69 · Initialisation et boucle de lecture

```java
61          try {
62              producer.initTransactions();
63              consumer.subscribe(List.of("transactions-in"));
64              System.out.println("transactions-in --(majuscules)--> transactions-out  (Ctrl+C pour arrêter)");
65              while (true) {
66                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
67                  if (records.isEmpty()) {
68                      continue;
69                  }
```

- **L61** — Ouvre le bloc `try` général.
- **L62** — Enregistre le producteur auprès du coordinateur de transactions.
- **L63** — Abonne le consommateur au topic d'entrée.
- **L64** — Affiche le rôle du programme.
- **L65** — Boucle infinie.
- **L66** — Récupère un lot de messages.
- **L67** — Si le lot est vide…
- **L68** — …revient au début de la boucle, sans ouvrir de transaction inutile.
- **L69** — Ferme le bloc ouvert ligne 67.

## Lignes 70 à 86 · Une transaction par lot

```java
70                  producer.beginTransaction();
71                  try {
72                      Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
73                      for (ConsumerRecord<String, String> r : records) {
74                          String transformed = r.value().toUpperCase();
75                          producer.send(new ProducerRecord<>("transactions-out", r.key(), transformed));
76                          offsets.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
77                          System.out.printf("lu [%s] -> écrit [%s]%n", r.value(), transformed);
78                      }
79                      producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
80                      producer.commitTransaction();
81                      System.out.println("  transaction commitée (" + records.count() + " messages)");
82                  } catch (KafkaException e) {
83                      producer.abortTransaction();
84                      System.err.println("  transaction annulée : " + e.getMessage());
85                  }
86              }
```

- **L70** — Démarre une transaction pour ce lot.
- **L71** — Ouvre le bloc `try` de la transaction.
- **L72** — Table des offsets à valider, par partition.
- **L73** — Parcourt les messages du lot.
- **L74** — Transformation : la valeur est mise en majuscules.
- **L75** — Envoie le résultat dans `transactions-out`, avec la même clé.
- **L76** — Note pour la partition du message l'offset suivant (offset lu + 1) : c'est lui qui sera validé.
- **L77** — Affiche la valeur lue et la valeur écrite.
- **L78** — Ferme le bloc ouvert ligne 73.
- **L79** — Ajoute à la transaction la validation des offsets du groupe : `groupMetadata()` transmet l'identité du groupe et sa génération, ce qui protège contre un consommateur « zombie ».
- **L80** — Valide la transaction : messages produits et offsets consommés deviennent visibles ensemble, de façon atomique.
- **L81** — Affiche le nombre de messages de la transaction.
- **L82** — Intercepte une exception Kafka survenue pendant la transaction.
- **L83** — Annule la transaction : ni les messages produits ni les offsets ne sont validés. À noter : la position en mémoire du consommateur a déjà avancé ; ce lot ne sera donc relu qu'après un redémarrage ou un rééquilibrage (un code de production repositionnerait le consommateur avec `seek` sur les derniers offsets validés).
- **L84** — Affiche la cause de l'annulation.
- **L85** — Ferme le bloc ouvert ligne 82.
- **L86** — Ferme le bloc ouvert ligne 65.

## Lignes 87 à 95 · Arrêt et fermeture

```java
87          } catch (WakeupException e) {
88              // arrêt demandé
89          } finally {
90              producer.close();
91              consumer.close();
92              System.out.println("Processeur arrêté.");
93          }
94      }
95  }
```

- **L87** — Intercepte la `WakeupException` de l'arrêt demandé.
- **L88** — Commentaire : rien à faire.
- **L89** — Bloc `finally`, exécuté dans tous les cas.
- **L90** — Ferme le producteur (une transaction encore ouverte serait annulée).
- **L91** — Ferme le consommateur, qui quitte le groupe.
- **L92** — Confirme l'arrêt.
- **L93** — Ferme le bloc ouvert ligne 89.
- **L94** — Ferme le bloc ouvert ligne 35.
- **L95** — Ferme le bloc ouvert ligne 33.
