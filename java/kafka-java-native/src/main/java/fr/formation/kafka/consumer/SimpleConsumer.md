# `SimpleConsumer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/consumer/SimpleConsumer.java` · 69 lignes

Premier consommateur du TP03 : il lit un topic au sein d'un groupe de consommateurs, avec validation automatique des offsets et arrêt propre sur Ctrl+C.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 13 · Package et imports

```java
 1  package fr.formation.kafka.consumer;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.consumer.ConsumerConfig;
 5  import org.apache.kafka.clients.consumer.ConsumerRecord;
 6  import org.apache.kafka.clients.consumer.ConsumerRecords;
 7  import org.apache.kafka.clients.consumer.KafkaConsumer;
 8  import org.apache.kafka.common.errors.WakeupException;
 9  import org.apache.kafka.common.serialization.StringDeserializer;
10  
11  import java.time.Duration;
12  import java.util.List;
13  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.consumer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `ConsumerConfig` : constantes des noms de paramètres du consommateur.
- **L5** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L6** — Importe `ConsumerRecords` : le lot de messages rendu par un appel à `poll()`.
- **L7** — Importe `KafkaConsumer` : le client consommateur Kafka (non thread-safe : un consommateur par thread).
- **L8** — Importe `WakeupException` : exception levée dans `poll()` quand un autre thread a appelé `consumer.wakeup()`.
- **L9** — Importe `StringDeserializer` : désérialiseur qui convertit des octets (UTF-8) en `String`.
- **L11** — Importe `Duration` : durée (utilisée pour les délais de `poll()` et les fenêtres).
- **L12** — Importe `List` : interface des listes.
- **L13** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 15 à 23 · Documentation et déclaration de la classe

```java
15  /**
16   * TP03 - Consumer au sein d'un consumer group, commit automatique des offsets.
17   * <p>
18   * Lancez plusieurs instances avec le MEME groupe : les partitions du topic sont
19   * réparties entre elles (consommation en parallèle). Avec un groupe DIFFERENT,
20   * chaque instance reçoit tous les messages (publish / subscribe).
21   * Usage : ./run.sh consumer.SimpleConsumer [topic] [groupe]
22   */
23  public class SimpleConsumer {
```

- **L15–22** — Javadoc : lancer plusieurs instances avec le même groupe répartit les partitions entre elles (consommation en parallèle) ; avec des groupes différents, chaque instance reçoit tous les messages (publish / subscribe). Lancement : `./run.sh consumer.SimpleConsumer [topic] [groupe]`.
- **L23** — Déclare la classe publique `SimpleConsumer`.

## Lignes 25 à 27 · Point d'entrée et paramètres

```java
25      public static void main(String[] args) {
26          String topic = args.length > 0 ? args[0] : "test-formation";
27          String group = args.length > 1 ? args[1] : "formation-group";
```

- **L25** — Méthode `main`.
- **L26** — Topic à lire : premier argument, ou `test-formation`.
- **L27** — Groupe de consommateurs : second argument, ou `formation-group`.

## Lignes 29 à 38 · Configuration du consommateur

```java
29          Properties props = KafkaSettings.base("simple-consumer");
30          props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
31          // Un consumer DESERIALISE la clé et la valeur
32          props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
33          props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
34          // Si le groupe n'a encore aucun offset commité : lecture depuis le début
35          props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
36          // Commit automatique toutes les secondes (sémantique "au moins une fois" approximative)
37          props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
38          props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
```

- **L29** — Propriétés de base avec `client.id=simple-consumer`.
- **L30** — `group.id` : le groupe auquel appartient ce consommateur. Les partitions du topic sont partagées entre les membres d'un même groupe, et les offsets sont mémorisés par groupe.
- **L31** — Commentaire : un consommateur désérialise la clé et la valeur.
- **L32** — `key.deserializer` : octets → `String` pour la clé.
- **L33** — `value.deserializer` : octets → `String` pour la valeur.
- **L34** — Commentaire : cas où le groupe n'a encore aucun offset validé.
- **L35** — `auto.offset.reset=earliest` : sans offset validé pour le groupe, la lecture démarre au début de chaque partition (par défaut, `latest` : seuls les nouveaux messages).
- **L36** — Commentaire : validation automatique toutes les secondes, soit une sémantique « au moins une fois » approximative.
- **L37** — `enable.auto.commit=true` : le consommateur valide lui-même périodiquement les offsets des messages rendus par `poll()`.
- **L38** — `auto.commit.interval.ms=1000` : cette validation a lieu toutes les secondes (5 secondes par défaut).

## Lignes 40 à 50 · Création du consommateur et arrêt propre

```java
40          KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
41          // Arrêt propre sur Ctrl+C : wakeup() fait lever une WakeupException dans poll()
42          Thread mainThread = Thread.currentThread();
43          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
44              consumer.wakeup();
45              try {
46                  mainThread.join();
47              } catch (InterruptedException e) {
48                  Thread.currentThread().interrupt();
49              }
50          }));
```

- **L40** — Crée le consommateur, typé `<String, String>`.
- **L41** — Commentaire : `wakeup()` fait lever une `WakeupException` dans `poll()`, ce qui permet un arrêt propre.
- **L42** — Mémorise le thread principal, pour pouvoir attendre sa fin.
- **L43** — Enregistre un *shutdown hook*, exécuté par la JVM lors d'un Ctrl+C.
- **L44** — `wakeup()` est la seule méthode du consommateur utilisable depuis un autre thread : elle interrompt le `poll()` en cours.
- **L45** — Ouvre un bloc `try`.
- **L46** — Attend la fin du thread principal, donc la fermeture complète du consommateur, avant de laisser la JVM s'arrêter.
- **L47** — Intercepte l'interruption éventuelle de cette attente.
- **L48** — Rétablit l'indicateur d'interruption du thread, comme le veut la bonne pratique.
- **L49** — Ferme le bloc ouvert ligne 47.
- **L50** — Ferme le bloc ouvert ligne 43.

## Lignes 52 à 61 · Boucle de consommation

```java
52          try {
53              consumer.subscribe(List.of(topic));
54              System.out.printf("Consumer du groupe '%s' abonné à '%s' (Ctrl+C pour arrêter)%n", group, topic);
55              while (true) {
56                  ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
57                  for (ConsumerRecord<String, String> record : records) {
58                      System.out.printf("partition=%d offset=%d key=%s value=%s%n",
59                              record.partition(), record.offset(), record.key(), record.value());
60                  }
61              }
```

- **L52** — Ouvre le bloc `try` qui encadre toute la consommation.
- **L53** — S'abonne au topic : le consommateur rejoint le groupe et recevra des partitions lors du prochain `poll()`.
- **L54** — Affiche le groupe et le topic.
- **L55** — Boucle infinie : un consommateur tourne jusqu'à ce qu'on l'arrête.
- **L56** — `poll()` récupère un lot de messages, en attendant au plus une seconde s'il n'y en a pas. C'est aussi `poll()` qui gère l'adhésion au groupe et les validations automatiques.
- **L57** — Parcourt les messages du lot.
- **L58–59** — Affiche la partition, l'offset, la clé et la valeur de chaque message.
- **L60** — Ferme le bloc ouvert ligne 57.
- **L61** — Ferme le bloc ouvert ligne 55.

## Lignes 62 à 69 · Arrêt et fermeture

```java
62          } catch (WakeupException e) {
63              // arrêt demandé
64          } finally {
65              consumer.close();   // quitte le groupe proprement : rééquilibrage immédiat
66              System.out.println("Consumer fermé.");
67          }
68      }
69  }
```

- **L62** — Intercepte la `WakeupException` provoquée par le *shutdown hook*.
- **L63** — Commentaire : il n'y a rien à faire, l'arrêt a été demandé.
- **L64** — Bloc `finally`, exécuté dans tous les cas.
- **L65** — Ferme le consommateur : il quitte le groupe proprement, ce qui déclenche un rééquilibrage immédiat au lieu d'attendre l'expiration de sa session.
- **L66** — Confirme la fermeture.
- **L67** — Ferme le bloc ouvert ligne 64.
- **L68** — Ferme le bloc ouvert ligne 25.
- **L69** — Ferme le bloc ouvert ligne 23.
