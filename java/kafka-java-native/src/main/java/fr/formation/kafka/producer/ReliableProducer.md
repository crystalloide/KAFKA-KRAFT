# `ReliableProducer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/producer/ReliableProducer.java` · 74 lignes

Producteur « de production » du TP03 : il réunit les réglages de fiabilité (acquittements, idempotence, nouvelles tentatives) et de débit, puis envoie un message par seconde pour observer le comportement quand des brokers s'arrêtent.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 12 · Package et imports

```java
 1  package fr.formation.kafka.producer;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.producer.KafkaProducer;
 5  import org.apache.kafka.clients.producer.ProducerConfig;
 6  import org.apache.kafka.clients.producer.ProducerRecord;
 7  import org.apache.kafka.common.serialization.StringSerializer;
 8  
 9  import java.time.LocalTime;
10  import java.util.Properties;
11  import java.util.concurrent.atomic.AtomicBoolean;
12  import java.util.concurrent.atomic.AtomicLong;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L5** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L6** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L7** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L9** — Importe `LocalTime` : heure locale sans date.
- **L10** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.
- **L11** — Importe `AtomicBoolean` : booléen modifiable de façon atomique entre threads.
- **L12** — Importe `AtomicLong` : compteur entier long modifiable de façon atomique entre threads.

## Lignes 14 à 26 · Documentation et déclaration de la classe

```java
14  /**
15   * TP03 - Producer "production" : garanties de livraison.
16   * <ul>
17   *   <li>acks=all + min.insync.replicas=2 (topic) : écriture confirmée par 2 réplicas minimum</li>
18   *   <li>enable.idempotence=true : pas de doublon ni de désordre en cas de retry</li>
19   *   <li>retries + delivery.timeout.ms : on réessaie pendant 2 minutes maximum</li>
20   *   <li>linger.ms / batch.size / compression : débit (voir TP05)</li>
21   * </ul>
22   * Envoie 1 message par seconde jusqu'à Ctrl+C : arrêtez des brokers pendant
23   * l'exécution pour observer le comportement (voir TP02 et TP03).
24   * Usage : ./run.sh producer.ReliableProducer [topic] [acks]
25   */
26  public class ReliableProducer {
```

- **L14–25** — Javadoc : les garanties illustrées (`acks=all` avec `min.insync.replicas=2`, idempotence, nouvelles tentatives bornées à 2 minutes, réglages de débit) ; le programme envoie un message par seconde jusqu'à Ctrl+C. Lancement : `./run.sh producer.ReliableProducer [topic] [acks]`.
- **L26** — Déclare la classe publique `ReliableProducer`.

## Lignes 28 à 30 · Point d'entrée et paramètres

```java
28      public static void main(String[] args) throws InterruptedException {
29          String topic = args.length > 0 ? args[0] : "test-formation";
30          String acks = args.length > 1 ? args[1] : "all";
```

- **L28** — Méthode `main` ; `InterruptedException` peut être levée par `Thread.sleep`.
- **L29** — Topic cible : premier argument, ou `test-formation`.
- **L30** — Niveau d'acquittement : second argument (`0`, `1` ou `all`), ou `all` par défaut. Cela permet de comparer les comportements.

## Lignes 32 à 34 · Configuration : sérialisation

```java
32          Properties props = KafkaSettings.base("reliable-producer");
33          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
34          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

- **L32** — Propriétés de base avec `client.id=reliable-producer`.
- **L33** — Sérialiseur de la clé.
- **L34** — Sérialiseur de la valeur.

## Lignes 35 à 42 · Configuration : garanties de livraison

```java
35          // --- Garanties de livraison ---
36          props.put(ProducerConfig.ACKS_CONFIG, acks);
37          props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, acks.equals("all"));
38          props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
39          props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
40          props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
41          props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
42          props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
```

- **L35** — Commentaire de section.
- **L36** — `acks` : nombre d'acquittements attendus avant de considérer l'envoi comme réussi.
- **L37** — `enable.idempotence` : activée seulement si `acks=all`, car l'idempotence l'exige (sinon le producteur refuserait de démarrer). Elle évite les doublons et le désordre lors des renvois.
- **L38** — `max.in.flight.requests.per.connection=5` : au plus 5 requêtes en attente de réponse par connexion ; c'est le maximum compatible avec l'idempotence et la conservation de l'ordre.
- **L39** — `retries` : nombre de nouvelles tentatives pratiquement illimité…
- **L40** — …car la vraie limite est `delivery.timeout.ms` : 120 000 ms, soit 2 minutes au total pour livrer un message (tentatives comprises). Au-delà, le callback reçoit une exception.
- **L41** — `request.timeout.ms` : 30 secondes d'attente de la réponse du broker pour chaque requête.
- **L42** — `retry.backoff.ms` : 100 ms d'attente avant une nouvelle tentative.

## Lignes 43 à 46 · Configuration : débit

```java
43          // --- Débit ---
44          props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
45          props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
46          props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
```

- **L43** — Commentaire de section.
- **L44** — `linger.ms=20` : le producteur attend jusqu'à 20 ms pour regrouper plusieurs messages dans un même lot.
- **L45** — `batch.size` : taille maximale d'un lot, ici 64 Ko (16 Ko par défaut).
- **L46** — `compression.type=lz4` : chaque lot est compressé, ce qui réduit le trafic réseau et l'espace disque.

## Lignes 48 à 51 · Compteurs et arrêt propre

```java
48          AtomicBoolean running = new AtomicBoolean(true);
49          AtomicLong ok = new AtomicLong();
50          AtomicLong ko = new AtomicLong();
51          Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));
```

- **L48** — Drapeau « en cours d'exécution », atomique car lu et modifié par deux threads différents.
- **L49** — Compteur des messages acquittés.
- **L50** — Compteur des messages en échec.
- **L51** — Enregistre un *shutdown hook* : à l'arrêt de la JVM (Ctrl+C), un thread passe le drapeau à `false`, ce qui fait sortir de la boucle d'envoi.

## Lignes 53 à 71 · Boucle d'envoi

```java
53          System.out.printf("Envoi vers '%s' avec acks=%s (Ctrl+C pour arrêter)%n", topic, acks);
54          try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
55              long i = 0;
56              while (running.get()) {
57                  final long n = i++;
58                  String value = "message " + n + " envoyé à " + LocalTime.now();
59                  producer.send(new ProducerRecord<>(topic, "cle-" + (n % 6), value), (md, ex) -> {
60                      if (ex == null) {
61                          ok.incrementAndGet();
62                          System.out.printf("[OK] %-45s partition=%d offset=%d%n", value, md.partition(), md.offset());
63                      } else {
64                          ko.incrementAndGet();
65                          System.err.printf("[KO] message %d : %s - %s%n", n,
66                                  ex.getClass().getSimpleName(), ex.getMessage());
67                      }
68                  });
69                  Thread.sleep(1000);
70              }
71          }
```

- **L53** — Affiche le topic et le niveau d'acquittement utilisés.
- **L54** — Crée le producteur, fermé automatiquement en fin de bloc.
- **L55** — Numéro du prochain message.
- **L56** — Boucle tant que l'arrêt n'a pas été demandé.
- **L57** — Copie le numéro courant dans une variable `final` (utilisable dans la lambda) puis incrémente le compteur.
- **L58** — Construit la valeur du message : numéro et heure d'envoi.
- **L59** — Envoie le message avec une clé parmi six (`cle-0` à `cle-5`) et une fonction de rappel.
- **L60** — Si aucune exception : l'envoi est acquitté.
- **L61** — Incrémente le compteur de succès.
- **L62** — Affiche la valeur (cadrée à gauche sur 45 caractères), la partition et l'offset.
- **L63** — Sinon : l'envoi a définitivement échoué (après toutes les tentatives).
- **L64** — Incrémente le compteur d'échecs.
- **L65–66** — Affiche le numéro du message, le type de l'exception et son message.
- **L67** — Ferme le bloc ouvert ligne 63.
- **L68** — Ferme le bloc ouvert ligne 59.
- **L69** — Attend une seconde avant le message suivant.
- **L70** — Ferme le bloc ouvert ligne 56.
- **L71** — Ferme le bloc ouvert ligne 54.

## Lignes 72 à 74 · Bilan

```java
72          System.out.printf("Fin : %d acquittés, %d en échec%n", ok.get(), ko.get());
73      }
74  }
```

- **L72** — Après la fermeture du producteur, affiche le nombre de messages acquittés et en échec.
- **L73** — Ferme le bloc ouvert ligne 28.
- **L74** — Ferme le bloc ouvert ligne 26.
