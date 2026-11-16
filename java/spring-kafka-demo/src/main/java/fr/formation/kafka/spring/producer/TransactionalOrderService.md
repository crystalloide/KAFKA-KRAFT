# `TransactionalOrderService.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/producer/TransactionalOrderService.java` · 56 lignes

Service d'envoi transactionnel : la commande et sa ligne d'audit sont écrites dans deux topics de façon atomique, à l'aide d'un `KafkaTemplate` transactionnel construit par la classe elle-même.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 12 · Package et imports

```java
 1  package fr.formation.kafka.spring.producer;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.slf4j.Logger;
 5  import org.slf4j.LoggerFactory;
 6  import org.springframework.beans.factory.DisposableBean;
 7  import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
 8  import org.springframework.kafka.core.DefaultKafkaProducerFactory;
 9  import org.springframework.kafka.core.KafkaTemplate;
10  import org.springframework.stereotype.Service;
11  
12  import java.util.Map;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.producer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `Logger` : interface de journalisation SLF4J.
- **L5** — Importe `LoggerFactory` : fabrique des journaliseurs SLF4J.
- **L6** — Importe `DisposableBean` : interface Spring : méthode `destroy()` appelée à l'arrêt de l'application.
- **L7** — Importe `KafkaProperties` : objet Spring Boot contenant les propriétés `spring.kafka.*` de `application.yml`.
- **L8** — Importe `DefaultKafkaProducerFactory` : fabrique Spring de producteurs Kafka (gère aussi les producteurs transactionnels).
- **L9** — Importe `KafkaTemplate` : façade Spring pour envoyer des messages.
- **L10** — Importe `Service` : annotation : composant Spring de la couche « service ».
- **L12** — Importe `Map` : interface des tables clé → valeur.

## Lignes 14 à 22 · Documentation et déclaration de la classe

```java
14  /**
15   * Producer TRANSACTIONNEL : la commande (topic "orders") et sa ligne d'audit
16   * (topic "orders-audit") sont écrites de façon atomique.
17   * <p>
18   * Le KafkaTemplate transactionnel est construit ici (et non déclaré comme bean) pour
19   * conserver le KafkaTemplate non transactionnel auto-configuré par Spring Boot.
20   */
21  @Service
22  public class TransactionalOrderService implements DisposableBean {
```

- **L14–20** — Javadoc : producteur transactionnel ; la commande (topic `orders`) et sa ligne d'audit (topic `orders-audit`) sont écrites de façon atomique. Le template transactionnel est construit ici, et non déclaré comme bean, pour conserver le `KafkaTemplate` non transactionnel auto-configuré par Spring Boot.
- **L21** — `@Service` : composant Spring.
- **L22** — Déclare la classe ; elle implémente `DisposableBean` pour libérer ses ressources à l'arrêt de l'application.

## Lignes 24 à 27 · Champs

```java
24      private static final Logger log = LoggerFactory.getLogger(TransactionalOrderService.class);
25  
26      private final DefaultKafkaProducerFactory<String, Order> producerFactory;
27      private final KafkaTemplate<String, Order> transactionalTemplate;
```

- **L24** — Journaliseur de la classe.
- **L26** — Fabrique de producteurs propre à ce service.
- **L27** — `KafkaTemplate` transactionnel propre à ce service.

## Lignes 29 à 34 · Constructeur : fabrication du template transactionnel

```java
29      public TransactionalOrderService(KafkaProperties kafkaProperties) {
30          Map<String, Object> config = kafkaProperties.buildProducerProperties();
31          this.producerFactory = new DefaultKafkaProducerFactory<>(config);
32          this.producerFactory.setTransactionIdPrefix("order-tx-");
33          this.transactionalTemplate = new KafkaTemplate<>(producerFactory);
34      }
```

- **L29** — Constructeur : Spring y injecte les propriétés `spring.kafka.*`.
- **L30** — Construit la table de configuration d'un producteur à partir de ces propriétés (mêmes brokers et sérialiseurs que le template standard).
- **L31** — Crée une fabrique de producteurs avec cette configuration.
- **L32** — Lui donne un préfixe de `transactional.id` : c'est ce qui rend ses producteurs transactionnels (Spring complète le préfixe par un suffixe).
- **L33** — Crée le `KafkaTemplate` appuyé sur cette fabrique.
- **L34** — Ferme le bloc ouvert ligne 29.

## Lignes 36 à 50 · Méthode sendWithAudit() : deux envois dans une transaction

```java
36      /**
37       * @param fail si true, une exception est levée APRES les envois : la transaction
38       *             est annulée et aucun des deux messages n'est visible en read_committed.
39       */
40      public void sendWithAudit(Order order, boolean fail) {
41          transactionalTemplate.executeInTransaction(operations -> {
42              operations.send("orders", order.customerId(), order);
43              operations.send("orders-audit", order.orderId(), order);
44              if (fail) {
45                  throw new IllegalStateException("Echec simulé : la transaction est annulée");
46              }
47              return true;
48          });
49          log.info("Transaction commitée pour {}", order.orderId());
50      }
```

- **L36–39** — Javadoc du paramètre `fail` : s'il vaut `true`, une exception est levée après les envois ; la transaction est annulée et aucun des deux messages n'est visible en `read_committed`.
- **L40** — Déclare la méthode `sendWithAudit`.
- **L41** — `executeInTransaction` : démarre une transaction Kafka, exécute la lambda, puis valide la transaction, ou l'annule si la lambda lève une exception.
- **L42** — Premier envoi : la commande dans `orders`, clé = identifiant client.
- **L43** — Second envoi : la même commande dans `orders-audit`, clé = identifiant de commande.
- **L44** — Si l'échec simulé est demandé…
- **L45** — …lève une exception : la transaction est annulée et l'exception remonte à l'appelant.
- **L46** — Ferme le bloc ouvert ligne 44.
- **L47** — Valeur de retour de la lambda (exigée par la signature, non exploitée ici).
- **L48** — Ferme le bloc ouvert ligne 41.
- **L49** — Ligne atteinte seulement si la transaction a été validée : on le journalise.
- **L50** — Ferme le bloc ouvert ligne 40.

## Lignes 52 à 56 · Libération des ressources

```java
52      @Override
53      public void destroy() {
54          producerFactory.destroy();
55      }
56  }
```

- **L52** — `@Override`.
- **L53** — Méthode `destroy()`, appelée par Spring à l'arrêt de l'application.
- **L54** — Ferme la fabrique et les producteurs qu'elle a créés.
- **L55** — Ferme le bloc ouvert ligne 53.
- **L56** — Ferme le bloc ouvert ligne 22.
