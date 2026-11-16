# `OrderProducer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/producer/OrderProducer.java` · 60 lignes

Producteur du TP03 sur le partitionnement : il envoie les commandes du fichier CSV en JSON avec l'identifiant client comme clé, puis montre que chaque client retombe toujours dans la même partition.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 14 · Package et imports

```java
 1  package fr.formation.kafka.producer;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import fr.formation.kafka.common.Order;
 5  import org.apache.kafka.clients.producer.KafkaProducer;
 6  import org.apache.kafka.clients.producer.ProducerConfig;
 7  import org.apache.kafka.clients.producer.ProducerRecord;
 8  import org.apache.kafka.common.serialization.StringSerializer;
 9  
10  import java.util.List;
11  import java.util.Map;
12  import java.util.Properties;
13  import java.util.TreeMap;
14  import java.util.concurrent.ConcurrentHashMap;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `Order` : le *record* du projet représentant une commande (conversion JSON, lecture du CSV).
- **L5** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L6** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L7** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L8** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L10** — Importe `List` : interface des listes.
- **L11** — Importe `Map` : interface des tables clé → valeur.
- **L12** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.
- **L13** — Importe `TreeMap` : table clé → valeur triée par clé.
- **L14** — Importe `ConcurrentHashMap` : table de hachage utilisable par plusieurs threads.

## Lignes 16 à 24 · Documentation et déclaration de la classe

```java
16  /**
17   * TP03 - Partitionnement par clé et ordre des messages.
18   * <p>
19   * Envoie le jeu de données data/orders.csv au format JSON dans le topic "orders"
20   * avec customerId comme CLE : toutes les commandes d'un même client arrivent
21   * dans la même partition et sont donc lues dans l'ordre d'envoi.
22   * Usage : ./run.sh producer.OrderProducer [topic] [pause_ms]
23   */
24  public class OrderProducer {
```

- **L16–23** — Javadoc : les commandes de `data/orders.csv` sont envoyées dans le topic `orders` avec `customerId` comme clé ; toutes les commandes d'un même client arrivent donc dans la même partition et sont lues dans l'ordre d'envoi. Lancement : `./run.sh producer.OrderProducer [topic] [pause_ms]`.
- **L24** — Déclare la classe publique `OrderProducer`.

## Lignes 26 à 28 · Point d'entrée et paramètres

```java
26      public static void main(String[] args) throws InterruptedException {
27          String topic = args.length > 0 ? args[0] : "orders";
28          long pauseMs = args.length > 1 ? Long.parseLong(args[1]) : 0L;
```

- **L26** — Méthode `main` ; `InterruptedException` peut être levée par `Thread.sleep`.
- **L27** — Topic cible : premier argument, ou `orders` par défaut.
- **L28** — Pause entre deux envois, en millisecondes : second argument, ou 0 (aucune pause).

## Lignes 30 à 32 · Configuration du producteur

```java
30          Properties props = KafkaSettings.base("order-producer");
31          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
32          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
```

- **L30** — Propriétés de base avec `client.id=order-producer`.
- **L31** — Sérialiseur de la clé : `String` → octets.
- **L32** — Sérialiseur de la valeur : `String` (le JSON) → octets.

## Lignes 34 à 35 · Données à envoyer

```java
34          List<Order> orders = Order.loadCsv(KafkaSettings.dataFile("orders.csv"));
35          Map<String, Integer> partitionByCustomer = new ConcurrentHashMap<>();
```

- **L34** — Localise `orders.csv` puis le charge en liste de commandes.
- **L35** — Table client → partition, remplie par les callbacks. `ConcurrentHashMap` car les callbacks s'exécutent dans le thread d'envoi du producteur, pas dans le thread principal.

## Lignes 37 à 54 · Envoi des commandes

```java
37          try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
38              for (Order order : orders) {
39                  ProducerRecord<String, String> record =
40                          new ProducerRecord<>(topic, order.customerId(), order.toJson());
41                  producer.send(record, (md, ex) -> {
42                      if (ex != null) {
43                          System.err.println("ECHEC " + order.orderId() + " : " + ex.getMessage());
44                          return;
45                      }
46                      partitionByCustomer.put(order.customerId(), md.partition());
47                      System.out.printf("%s client=%s -> partition %d offset %d%n",
48                              order.orderId(), order.customerId(), md.partition(), md.offset());
49                  });
50                  if (pauseMs > 0) {
51                      Thread.sleep(pauseMs);
52                  }
53              }
54          }
```

- **L37** — Crée le producteur, fermé automatiquement en fin de bloc.
- **L38** — Parcourt les commandes.
- **L39–40** — Construit le message : topic, clé = identifiant du client, valeur = commande en JSON. C'est la clé qui détermine la partition (hachage de la clé modulo le nombre de partitions).
- **L41** — Envoie le message de façon asynchrone, avec une fonction de rappel recevant les métadonnées (`md`) ou l'exception (`ex`).
- **L42** — Teste l'échec de l'envoi.
- **L43** — Affiche la commande en échec et la cause.
- **L44** — Quitte la lambda : rien d'autre à faire pour ce message.
- **L45** — Ferme le bloc ouvert ligne 42.
- **L46** — En cas de succès, mémorise la partition obtenue pour ce client.
- **L47–48** — Affiche la commande, le client, la partition et l'offset.
- **L49** — Ferme le bloc ouvert ligne 41.
- **L50** — Si une pause a été demandée…
- **L51** — …attend ce délai, ce qui permet d'observer l'arrivée progressive des messages côté consommateur.
- **L52** — Ferme le bloc ouvert ligne 50.
- **L53** — Ferme le bloc ouvert ligne 38.
- **L54** — Ferme le bloc ouvert ligne 37.

## Lignes 55 à 60 · Bilan

```java
55          System.out.println("\n=== Partition retenue pour chaque clé (customerId) ===");
56          new TreeMap<>(partitionByCustomer).forEach((customer, partition) ->
57                  System.out.printf("  %s -> partition %d%n", customer, partition));
58          System.out.printf("%d commandes envoyées dans '%s'.%n", orders.size(), topic);
59      }
60  }
```

- **L55** — Affiche un titre, une fois le producteur fermé (tous les callbacks ont donc été exécutés).
- **L56** — Copie la table dans une `TreeMap` pour la trier par client, puis la parcourt.
- **L57** — Affiche pour chaque client la partition retenue : une seule partition par clé.
- **L58** — Affiche le nombre total de commandes envoyées et le topic.
- **L59** — Ferme le bloc ouvert ligne 26.
- **L60** — Ferme le bloc ouvert ligne 24.
