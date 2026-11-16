# `OrderProducerService.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/producer/OrderProducerService.java` · 40 lignes

Service d'envoi des commandes : il utilise le `KafkaTemplate` auto-configuré par Spring Boot et journalise le résultat de chaque envoi.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 10 · Package et imports

```java
 1  package fr.formation.kafka.spring.producer;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.slf4j.Logger;
 5  import org.slf4j.LoggerFactory;
 6  import org.springframework.kafka.core.KafkaTemplate;
 7  import org.springframework.kafka.support.SendResult;
 8  import org.springframework.stereotype.Service;
 9  
10  import java.util.concurrent.CompletableFuture;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `Logger` : interface de journalisation SLF4J.
- **L5** — Importe `LoggerFactory` : fabrique des journaliseurs SLF4J.
- **L6** — Importe `KafkaTemplate` : façade Spring pour envoyer des messages.
- **L7** — Importe `SendResult` : résultat d'un envoi : le message envoyé et son `RecordMetadata`.
- **L8** — Importe `Service` : annotation : composant Spring de la couche « service ».
- **L10** — Importe `CompletableFuture` : résultat futur d'une opération asynchrone, avec enchaînement de traitements.

## Lignes 12 à 17 · Documentation et déclaration de la classe

```java
12  /**
13   * Envoi des commandes avec le KafkaTemplate auto-configuré par Spring Boot
14   * (propriétés spring.kafka.producer.* de application.yml).
15   */
16  @Service
17  public class OrderProducerService {
```

- **L12–15** — Javadoc : envoi des commandes avec le `KafkaTemplate` auto-configuré à partir des propriétés `spring.kafka.producer.*` de `application.yml`.
- **L16** — `@Service` : Spring instancie cette classe et la rend injectable.
- **L17** — Déclare la classe publique `OrderProducerService`.

## Lignes 19 à 26 · Constantes et dépendance

```java
19      public static final String TOPIC = "orders";
20      private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
21  
22      private final KafkaTemplate<String, Order> kafkaTemplate;
23  
24      public OrderProducerService(KafkaTemplate<String, Order> kafkaTemplate) {
25          this.kafkaTemplate = kafkaTemplate;
26      }
```

- **L19** — Constante publique : le nom du topic des commandes.
- **L20** — Journaliseur SLF4J de la classe.
- **L22** — Champ recevant le `KafkaTemplate` (clé `String`, valeur `Order`) ; `final` : il est fixé une fois pour toutes.
- **L24** — Constructeur : Spring y injecte le `KafkaTemplate` (injection par constructeur).
- **L25** — Mémorise le template dans le champ.
- **L26** — Ferme le bloc ouvert ligne 24.

## Lignes 28 à 40 · Méthode send() : envoi asynchrone

```java
28      /** Envoi asynchrone : la clé customerId garantit l'ordre des commandes d'un même client. */
29      public CompletableFuture<SendResult<String, Order>> send(Order order) {
30          return kafkaTemplate.send(TOPIC, order.customerId(), order)
31                  .whenComplete((result, ex) -> {
32                      if (ex != null) {
33                          log.error("Echec de l'envoi de {} : {}", order.orderId(), ex.getMessage());
34                      } else {
35                          log.info("Commande {} envoyée -> partition {} offset {}", order.orderId(),
36                                  result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
37                      }
38                  });
39      }
40  }
```

- **L28** — Javadoc : envoi asynchrone ; la clé `customerId` garantit l'ordre des commandes d'un même client.
- **L29** — Déclare la méthode `send`, qui renvoie un `CompletableFuture` du résultat de l'envoi : l'appelant peut attendre ce résultat ou non.
- **L30** — Envoie la commande dans le topic avec l'identifiant client comme clé. L'objet `Order` est converti en JSON par le sérialiseur configuré dans `application.yml`.
- **L31** — `whenComplete` : enregistre un traitement exécuté à la fin de l'envoi, qu'il réussisse ou échoue.
- **L32** — Si une exception est présente…
- **L33** — …journalise l'échec en erreur, avec l'identifiant de la commande et la cause.
- **L34** — Sinon…
- **L35–36** — …journalise le succès avec la partition et l'offset obtenus, lus dans le `RecordMetadata`.
- **L37** — Ferme le bloc ouvert ligne 34.
- **L38** — Ferme le bloc ouvert ligne 31.
- **L39** — Ferme le bloc ouvert ligne 29.
- **L40** — Ferme le bloc ouvert ligne 17.
