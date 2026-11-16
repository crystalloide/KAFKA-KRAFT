# `VipOrderProducer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/producer/VipOrderProducer.java` · 37 lignes

Producteur du TP03 qui utilise un partitionneur personnalisé : il envoie les commandes du CSV en confiant le choix de la partition à la classe `VipPartitioner`.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 10 · Package et imports

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
10  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `Order` : le *record* du projet représentant une commande (conversion JSON, lecture du CSV).
- **L5** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L6** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L7** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L8** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L10** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 12 à 16 · Documentation et déclaration de la classe

```java
12  /**
13   * TP03 - Utilisation d'un partitionneur personnalisé (VipPartitioner).
14   * Usage : ./run.sh producer.VipOrderProducer [topic]
15   */
16  public class VipOrderProducer {
```

- **L12–15** — Javadoc : utilisation du partitionneur personnalisé `VipPartitioner` ; lancement par `./run.sh producer.VipOrderProducer [topic]`.
- **L16** — Déclare la classe publique `VipOrderProducer`.

## Lignes 18 à 24 · Point d'entrée et configuration

```java
18      public static void main(String[] args) {
19          String topic = args.length > 0 ? args[0] : "orders";
20  
21          Properties props = KafkaSettings.base("vip-order-producer");
22          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
23          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
24          props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VipPartitioner.class.getName());
```

- **L18** — Méthode `main`.
- **L19** — Topic cible : premier argument, ou `orders` par défaut.
- **L21** — Propriétés de base avec `client.id=vip-order-producer`.
- **L22** — Sérialiseur de la clé.
- **L23** — Sérialiseur de la valeur.
- **L24** — `partitioner.class` : remplace le partitionneur par défaut de Kafka par la classe `VipPartitioner` du projet. Le producteur l'instanciera lui-même à partir de son nom.

## Lignes 26 à 37 · Envoi des commandes

```java
26          try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
27              for (Order order : Order.loadCsv(KafkaSettings.dataFile("orders.csv"))) {
28                  producer.send(new ProducerRecord<>(topic, order.customerId(), order.toJson()), (md, ex) -> {
29                      if (ex == null) {
30                          System.out.printf("%s client=%s -> partition %d%n",
31                                  order.orderId(), order.customerId(), md.partition());
32                      }
33                  });
34              }
35          }
36      }
37  }
```

- **L26** — Crée le producteur, fermé automatiquement en fin de bloc.
- **L27** — Charge le fichier `orders.csv` et parcourt les commandes.
- **L28** — Envoie chaque commande (clé = client, valeur = JSON) avec une fonction de rappel. La partition est choisie par `VipPartitioner`.
- **L29** — Si l'envoi a réussi…
- **L30–31** — …affiche la commande, le client et la partition obtenue : on vérifie ainsi que les clients VIP vont en partition 0. (Un échec n'est pas signalé dans cet exemple.)
- **L32** — Ferme le bloc ouvert ligne 29.
- **L33** — Ferme le bloc ouvert ligne 28.
- **L34** — Ferme le bloc ouvert ligne 27.
- **L35** — Ferme le bloc ouvert ligne 26.
- **L36** — Ferme le bloc ouvert ligne 18.
- **L37** — Ferme le bloc ouvert ligne 16.
