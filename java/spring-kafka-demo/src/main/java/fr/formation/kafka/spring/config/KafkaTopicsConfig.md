# `KafkaTopicsConfig.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/config/KafkaTopicsConfig.java` · 36 lignes

Déclaration des topics de l'application sous forme de beans : Spring les crée au démarrage s'ils n'existent pas.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 6 · Package et imports

```java
 1  package fr.formation.kafka.spring.config;
 2  
 3  import org.apache.kafka.clients.admin.NewTopic;
 4  import org.springframework.context.annotation.Bean;
 5  import org.springframework.context.annotation.Configuration;
 6  import org.springframework.kafka.config.TopicBuilder;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.config`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `NewTopic` : description d'un topic à créer : nom, nombre de partitions, facteur de réplication, configurations.
- **L4** — Importe `Bean` : annotation : la méthode fournit un objet (bean) géré par Spring.
- **L5** — Importe `Configuration` : annotation : la classe déclare des beans.
- **L6** — Importe `TopicBuilder` : constructeur fluide d'objets `NewTopic`.

## Lignes 8 à 13 · Documentation et déclaration de la classe

```java
 8  /**
 9   * Déclaration des topics : KafkaAdmin (auto-configuré par Spring Boot) les crée au
10   * démarrage s'ils n'existent pas ("topics as code").
11   */
12  @Configuration
13  public class KafkaTopicsConfig {
```

- **L8–11** — Javadoc : `KafkaAdmin`, auto-configuré par Spring Boot, crée au démarrage les topics déclarés ici s'ils n'existent pas (« topics as code »).
- **L12** — `@Configuration` : la classe déclare des beans.
- **L13** — Déclare la classe publique `KafkaTopicsConfig`.

## Lignes 15 à 19 · Topic principal « orders »

```java
15      @Bean
16      NewTopic ordersTopic() {
17          return TopicBuilder.name("orders").partitions(6).replicas(3)
18                  .config("min.insync.replicas", "2").build();
19      }
```

- **L15** — `@Bean` : l'objet renvoyé par la méthode est enregistré dans le contexte Spring.
- **L16** — Méthode déclarant le topic `orders`.
- **L17** — Construit un `NewTopic` nommé `orders`, avec 6 partitions et un facteur de réplication de 3…
- **L18** — …et la configuration `min.insync.replicas=2` ; `build()` crée l'objet.
- **L19** — Ferme le bloc ouvert ligne 16.

## Lignes 21 à 25 · Dead letter topic « orders-dlt »

```java
21      /** Dead Letter Topic : même nombre de partitions que "orders" (même partition réutilisée). */
22      @Bean
23      NewTopic ordersDltTopic() {
24          return TopicBuilder.name("orders-dlt").partitions(6).replicas(3).build();
25      }
```

- **L21** — Javadoc : le *dead letter topic* a le même nombre de partitions que `orders`, car un message en échec y est republié dans la partition de même numéro.
- **L22** — `@Bean`.
- **L23** — Méthode déclarant le topic `orders-dlt`.
- **L24** — 6 partitions, réplication 3.
- **L25** — Ferme le bloc ouvert ligne 23.

## Lignes 27 à 36 · Topics « orders-audit » et « orders-big »

```java
27      @Bean
28      NewTopic ordersAuditTopic() {
29          return TopicBuilder.name("orders-audit").partitions(3).replicas(3).build();
30      }
31  
32      @Bean
33      NewTopic ordersBigTopic() {
34          return TopicBuilder.name("orders-big").partitions(3).replicas(3).build();
35      }
36  }
```

- **L27** — `@Bean`.
- **L28** — Méthode déclarant le topic `orders-audit` (utilisé par le service transactionnel).
- **L29** — 3 partitions, réplication 3.
- **L30** — Ferme le bloc ouvert ligne 28.
- **L32** — `@Bean`.
- **L33** — Méthode déclarant le topic `orders-big` (alimenté par Kafka Streams).
- **L34** — 3 partitions, réplication 3.
- **L35** — Ferme le bloc ouvert ligne 33.
- **L36** — Ferme le bloc ouvert ligne 13.
