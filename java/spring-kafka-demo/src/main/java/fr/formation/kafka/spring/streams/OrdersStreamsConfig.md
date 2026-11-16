# `OrdersStreamsConfig.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/streams/OrdersStreamsConfig.java` · 39 lignes

Topologie Kafka Streams intégrée à Spring (TP07b), activable par propriété : elle recopie les commandes d'au moins 500 EUR dans le topic `orders-big`.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 15 · Package et imports

```java
 1  package fr.formation.kafka.spring.streams;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.apache.kafka.common.serialization.Serdes;
 5  import org.apache.kafka.streams.StreamsBuilder;
 6  import org.apache.kafka.streams.kstream.Consumed;
 7  import org.apache.kafka.streams.kstream.KStream;
 8  import org.apache.kafka.streams.kstream.Produced;
 9  import org.slf4j.Logger;
10  import org.slf4j.LoggerFactory;
11  import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
12  import org.springframework.context.annotation.Bean;
13  import org.springframework.context.annotation.Configuration;
14  import org.springframework.kafka.annotation.EnableKafkaStreams;
15  import org.springframework.kafka.support.serializer.JacksonJsonSerde;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `Serdes` : fabrique de *serdes* (sérialiseur + désérialiseur) pour les types courants.
- **L5** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L6** — Importe `Consumed` : précise comment lire un topic source (serdes de clé et de valeur).
- **L7** — Importe `KStream` : flux d'enregistrements : chaque message est un fait indépendant.
- **L8** — Importe `Produced` : précise comment écrire dans un topic de sortie (serdes de clé et de valeur).
- **L9** — Importe `Logger` : interface de journalisation SLF4J.
- **L10** — Importe `LoggerFactory` : fabrique des journaliseurs SLF4J.
- **L11** — Importe `ConditionalOnProperty` : annotation : n'active la classe que si une propriété de configuration a la valeur attendue.
- **L12** — Importe `Bean` : annotation : la méthode fournit un objet (bean) géré par Spring.
- **L13** — Importe `Configuration` : annotation : la classe déclare des beans.
- **L14** — Importe `EnableKafkaStreams` : annotation : active le support Kafka Streams de Spring (création du `StreamsBuilder`).
- **L15** — Importe `JacksonJsonSerde` : serde JSON (Jackson 3) de Spring for Apache Kafka 4, utilisable avec Kafka Streams.

## Lignes 17 à 27 · Documentation et annotations de la classe

```java
17  /**
18   * Kafka Streams avec Spring (TP07b) : Spring Boot crée et pilote le StreamsBuilder
19   * (application-id = spring.kafka.streams.application-id).
20   * Activation : mvn spring-boot:run -Dspring-boot.run.arguments=--app.streams.enabled=true
21   */
22  @Configuration
23  @EnableKafkaStreams
24  @ConditionalOnProperty(name = "app.streams.enabled", havingValue = "true")
25  public class OrdersStreamsConfig {
26  
27      private static final Logger log = LoggerFactory.getLogger(OrdersStreamsConfig.class);
```

- **L17–21** — Javadoc : avec Spring, c'est Spring Boot qui crée et pilote le `StreamsBuilder` (l'identifiant d'application vient de `spring.kafka.streams.application-id`). Activation : `mvn spring-boot:run -Dspring-boot.run.arguments=--app.streams.enabled=true`.
- **L22** — `@Configuration` : la classe déclare des beans.
- **L23** — `@EnableKafkaStreams` : active le support Kafka Streams ; Spring fournit le `StreamsBuilder` et démarre / arrête l'application Streams avec le contexte.
- **L24** — `@ConditionalOnProperty` : la classe n'est prise en compte que si `app.streams.enabled` vaut `true`. Par défaut, Kafka Streams reste donc désactivé.
- **L25** — Déclare la classe publique `OrdersStreamsConfig`.
- **L27** — Journaliseur de la classe.

## Lignes 29 à 39 · La topologie

```java
29      @Bean
30      KStream<String, Order> bigOrdersStream(StreamsBuilder builder) {
31          JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
32          KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));
33          orders.filter((customer, order) -> order != null && order.amount() >= 500)
34                  .peek((customer, order) -> log.info("[Streams] grosse commande {} : {} EUR",
35                          order.orderId(), order.amount()))
36                  .to("orders-big", Produced.with(Serdes.String(), orderSerde));
37          return orders;
38      }
39  }
```

- **L29** — `@Bean`.
- **L30** — Méthode déclarant la topologie ; Spring lui injecte le `StreamsBuilder`.
- **L31** — Crée un serde JSON pour le type `Order`.
- **L32** — Source : lit le topic `orders`, clé `String`, valeur désérialisée en `Order` grâce à `Consumed.with`.
- **L33** — `filter` : ne garde que les commandes non nulles dont le montant atteint 500.
- **L34–35** — `peek` : journalise chaque grosse commande (identifiant et montant).
- **L36** — Écrit ces commandes dans `orders-big`, en JSON, avec les serdes indiqués par `Produced.with`.
- **L37** — Renvoie le flux source : il devient un bean Spring. C'est la construction de la topologie, déclenchée par cette méthode, qui importe.
- **L38** — Ferme le bloc ouvert ligne 30.
- **L39** — Ferme le bloc ouvert ligne 25.
