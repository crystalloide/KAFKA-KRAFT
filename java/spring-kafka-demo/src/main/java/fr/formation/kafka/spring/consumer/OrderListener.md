# `OrderListener.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/consumer/OrderListener.java` · 32 lignes

Consommateur principal de l'application : il reçoit les commandes du topic `orders`, les traite et valide manuellement les offsets ; une commande invalide déclenche la gestion d'erreurs.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 9 · Package et imports

```java
 1  package fr.formation.kafka.spring.consumer;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.apache.kafka.clients.consumer.ConsumerRecord;
 5  import org.slf4j.Logger;
 6  import org.slf4j.LoggerFactory;
 7  import org.springframework.kafka.annotation.KafkaListener;
 8  import org.springframework.kafka.support.Acknowledgment;
 9  import org.springframework.stereotype.Component;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.consumer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L5** — Importe `Logger` : interface de journalisation SLF4J.
- **L6** — Importe `LoggerFactory` : fabrique des journaliseurs SLF4J.
- **L7** — Importe `KafkaListener` : annotation : la méthode est appelée pour chaque message reçu d'un topic.
- **L8** — Importe `Acknowledgment` : objet permettant au listener de valider (commit) manuellement l'offset.
- **L9** — Importe `Component` : annotation : la classe est un composant instancié et géré par Spring.

## Lignes 11 à 19 · Documentation et déclaration de la classe

```java
11  /**
12   * Consumer du groupe "order-service" : 3 threads (concurrency) se partagent les
13   * 6 partitions du topic "orders". Commit manuel après traitement (ack-mode: manual).
14   * Une commande de quantité <= 0 provoque une exception -> retries -> "orders-dlt".
15   */
16  @Component
17  public class OrderListener {
18  
19      private static final Logger log = LoggerFactory.getLogger(OrderListener.class);
```

- **L11–15** — Javadoc : consommateur du groupe `order-service` ; 3 threads se partagent les 6 partitions du topic `orders` ; validation manuelle après traitement (`ack-mode: manual`). Une commande de quantité négative ou nulle provoque une exception, puis les nouvelles tentatives, puis l'envoi dans `orders-dlt`.
- **L16** — `@Component` : Spring instancie la classe et détecte ses méthodes annotées.
- **L17** — Déclare la classe publique `OrderListener`.
- **L19** — Journaliseur de la classe.

## Lignes 21 à 32 · Le listener

```java
21      @KafkaListener(id = "order-listener", topics = "orders", groupId = "order-service", concurrency = "3")
22      public void onOrder(ConsumerRecord<String, Order> record, Acknowledgment ack) {
23          Order order = record.value();
24          log.info("Reçu {} (client {}, {} x {}) partition={} offset={}", order.orderId(), order.customerId(),
25                  order.quantity(), order.product(), record.partition(), record.offset());
26          if (order.quantity() <= 0) {
27              throw new IllegalArgumentException("Quantité invalide pour la commande " + order.orderId());
28          }
29          // ... traitement métier ...
30          ack.acknowledge();
31      }
32  }
```

- **L21** — `@KafkaListener` : Spring crée un conteneur de listeners nommé `order-listener`, abonné au topic `orders` dans le groupe `order-service`, avec 3 consommateurs en parallèle (`concurrency`), soit 3 threads.
- **L22** — Méthode appelée pour chaque message. Elle reçoit le `ConsumerRecord` complet (valeur déjà désérialisée en `Order`) et l'objet `Acknowledgment`, disponible parce que le mode d'acquittement est manuel.
- **L23** — Extrait la commande du message.
- **L24–25** — Journalise la commande reçue, avec la partition et l'offset.
- **L26** — Contrôle métier : si la quantité est négative ou nulle…
- **L27** — …lève une exception. Le gestionnaire d'erreurs (`KafkaErrorHandlingConfig`) rejouera le message, puis l'enverra dans le *dead letter topic*.
- **L28** — Ferme le bloc ouvert ligne 26.
- **L29** — Commentaire : emplacement du traitement métier réel.
- **L30** — Valide l'offset du message : il est considéré comme traité. Cette ligne n'est pas atteinte en cas d'exception.
- **L31** — Ferme le bloc ouvert ligne 22.
- **L32** — Ferme le bloc ouvert ligne 17.
