# `OrderController.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/web/OrderController.java` · 82 lignes

API REST de l'application : trois points d'entrée POST pour envoyer une commande, générer des commandes aléatoires et tester l'envoi transactionnel.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 19 · Package et imports

```java
 1  package fr.formation.kafka.spring.web;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import fr.formation.kafka.spring.producer.OrderProducerService;
 5  import fr.formation.kafka.spring.producer.TransactionalOrderService;
 6  import org.springframework.http.ResponseEntity;
 7  import org.springframework.kafka.support.SendResult;
 8  import org.springframework.web.bind.annotation.PostMapping;
 9  import org.springframework.web.bind.annotation.RequestBody;
10  import org.springframework.web.bind.annotation.RequestMapping;
11  import org.springframework.web.bind.annotation.RequestParam;
12  import org.springframework.web.bind.annotation.RestController;
13  
14  import java.util.ArrayList;
15  import java.util.LinkedHashMap;
16  import java.util.List;
17  import java.util.Map;
18  import java.util.concurrent.ThreadLocalRandom;
19  import java.util.concurrent.TimeUnit;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.web`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `OrderProducerService` : service du projet qui envoie les commandes avec `KafkaTemplate`.
- **L5** — Importe `TransactionalOrderService` : service du projet qui envoie commande + audit dans une transaction Kafka.
- **L6** — Importe `ResponseEntity` : réponse HTTP complète (statut, en-têtes, corps).
- **L7** — Importe `SendResult` : résultat d'un envoi : le message envoyé et son `RecordMetadata`.
- **L8** — Importe `PostMapping` : annotation : associe la méthode aux requêtes HTTP POST.
- **L9** — Importe `RequestBody` : annotation : le paramètre est construit à partir du corps (JSON) de la requête.
- **L10** — Importe `RequestMapping` : annotation : préfixe d'URL commun aux méthodes du contrôleur.
- **L11** — Importe `RequestParam` : annotation : le paramètre vient de la chaîne de requête de l'URL (`?nom=valeur`).
- **L12** — Importe `RestController` : annotation : contrôleur web dont les retours sont sérialisés en JSON.
- **L14** — Importe `ArrayList` : liste modifiable.
- **L15** — Importe `LinkedHashMap` : table clé → valeur qui conserve l'ordre d'insertion.
- **L16** — Importe `List` : interface des listes.
- **L17** — Importe `Map` : interface des tables clé → valeur.
- **L18** — Importe `ThreadLocalRandom` : générateur de nombres aléatoires propre à chaque thread.
- **L19** — Importe `TimeUnit` : unités de temps (secondes, millisecondes…).

## Lignes 21 à 31 · Documentation et annotations de la classe

```java
21  /**
22   * API REST :
23   * <ul>
24   *   <li>POST /api/orders                      : envoie la commande reçue en JSON</li>
25   *   <li>POST /api/orders/random?count=20      : génère des commandes aléatoires</li>
26   *   <li>POST /api/orders/transaction?fail=... : commande + audit dans une transaction</li>
27   * </ul>
28   */
29  @RestController
30  @RequestMapping("/api/orders")
31  public class OrderController {
```

- **L21–28** — Javadoc : les trois points d'entrée — `POST /api/orders` (envoie la commande reçue en JSON), `POST /api/orders/random?count=20` (génère des commandes aléatoires), `POST /api/orders/transaction?fail=...` (commande + audit dans une transaction).
- **L29** — `@RestController` : contrôleur web dont les valeurs de retour sont converties en JSON dans la réponse HTTP.
- **L30** — `@RequestMapping("/api/orders")` : préfixe d'URL de toutes les méthodes de la classe.
- **L31** — Déclare la classe publique `OrderController`.

## Lignes 33 à 35 · Données de génération aléatoire

```java
33      private static final String[] PRODUCTS = {"Clavier", "Souris", "Ecran 27p", "Casque", "Laptop", "Dock USB-C"};
34      private static final double[] PRICES = {49.9, 19.9, 289.0, 89.0, 1299.0, 129.0};
35      private static final String[] COUNTRIES = {"FR", "BE", "CH", "CA", "DE"};
```

- **L33** — Tableau constant des produits possibles.
- **L34** — Tableau des prix, dans le même ordre que les produits (même indice).
- **L35** — Tableau des pays possibles.

## Lignes 37 à 43 · Dépendances

```java
37      private final OrderProducerService producer;
38      private final TransactionalOrderService transactionalProducer;
39  
40      public OrderController(OrderProducerService producer, TransactionalOrderService transactionalProducer) {
41          this.producer = producer;
42          this.transactionalProducer = transactionalProducer;
43      }
```

- **L37** — Service d'envoi standard.
- **L38** — Service d'envoi transactionnel.
- **L40** — Constructeur : Spring y injecte les deux services.
- **L41** — Mémorise le premier.
- **L42** — Mémorise le second.
- **L43** — Ferme le bloc ouvert ligne 40.

## Lignes 45 à 55 · POST /api/orders : envoyer une commande

```java
45      @PostMapping
46      public ResponseEntity<Map<String, Object>> create(@RequestBody Order request) throws Exception {
47          Order order = request.withDefaults();
48          SendResult<String, Order> result = producer.send(order).get(10, TimeUnit.SECONDS);
49          Map<String, Object> body = new LinkedHashMap<>();
50          body.put("orderId", order.orderId());
51          body.put("topic", result.getRecordMetadata().topic());
52          body.put("partition", result.getRecordMetadata().partition());
53          body.put("offset", result.getRecordMetadata().offset());
54          return ResponseEntity.ok(body);
55      }
```

- **L45** — `@PostMapping` sans chemin : la méthode répond à `POST /api/orders`.
- **L46** — Méthode `create`. `@RequestBody` : la commande est construite à partir du JSON du corps de la requête. `throws Exception` : l'attente du résultat peut échouer.
- **L47** — Complète la commande reçue (identifiant, pays, horodatage par défaut).
- **L48** — Envoie la commande puis attend le résultat au plus 10 secondes : l'envoi est rendu synchrone pour pouvoir renvoyer la partition et l'offset à l'appelant.
- **L49** — Crée le corps de la réponse, une table qui conserve l'ordre d'insertion.
- **L50** — Y place l'identifiant de la commande.
- **L51** — Le topic d'écriture.
- **L52** — La partition.
- **L53** — L'offset.
- **L54** — Renvoie une réponse HTTP 200 contenant cette table, sérialisée en JSON.
- **L55** — Ferme le bloc ouvert ligne 46.

## Lignes 57 à 69 · POST /api/orders/random : générer des commandes

```java
57      @PostMapping("/random")
58      public Map<String, Object> random(@RequestParam(defaultValue = "10") int count) {
59          List<String> ids = new ArrayList<>();
60          ThreadLocalRandom rnd = ThreadLocalRandom.current();
61          for (int i = 0; i < count; i++) {
62              int p = rnd.nextInt(PRODUCTS.length);
63              Order order = new Order(null, "C-%03d".formatted(rnd.nextInt(1, 11)), PRODUCTS[p],
64                      rnd.nextInt(1, 6), PRICES[p], COUNTRIES[rnd.nextInt(COUNTRIES.length)], 0L).withDefaults();
65              producer.send(order);
66              ids.add(order.orderId());
67          }
68          return Map.of("sent", count, "orderIds", ids);
69      }
```

- **L57** — `@PostMapping("/random")` : la méthode répond à `POST /api/orders/random`.
- **L58** — Méthode `random`. `@RequestParam` : le paramètre d'URL `count` donne le nombre de commandes, 10 par défaut.
- **L59** — Liste des identifiants générés.
- **L60** — Récupère le générateur aléatoire du thread courant.
- **L61** — Boucle sur le nombre de commandes.
- **L62** — Tire au hasard l'indice d'un produit.
- **L63** — Crée une commande : pas d'identifiant (`null`), un client de `C-001` à `C-010` (entier de 1 à 10 formaté sur 3 chiffres), le produit tiré…
- **L64** — …une quantité de 1 à 5, le prix du produit, un pays au hasard et un horodatage à 0 ; `withDefaults()` génère ensuite l'identifiant et l'horodatage.
- **L65** — Envoie la commande sans attendre le résultat (envoi asynchrone).
- **L66** — Ajoute son identifiant à la liste.
- **L67** — Ferme le bloc ouvert ligne 61.
- **L68** — Renvoie une table non modifiable contenant le nombre de commandes envoyées et leurs identifiants, convertie en JSON.
- **L69** — Ferme le bloc ouvert ligne 58.

## Lignes 71 à 82 · POST /api/orders/transaction : envoi transactionnel

```java
71      @PostMapping("/transaction")
72      public Map<String, Object> transaction(@RequestBody Order request,
73                                             @RequestParam(defaultValue = "false") boolean fail) {
74          Order order = request.withDefaults();
75          try {
76              transactionalProducer.sendWithAudit(order, fail);
77              return Map.of("orderId", order.orderId(), "transaction", "COMMIT");
78          } catch (RuntimeException e) {
79              return Map.of("orderId", order.orderId(), "transaction", "ABORT", "cause", String.valueOf(e.getMessage()));
80          }
81      }
82  }
```

- **L71** — `@PostMapping("/transaction")` : la méthode répond à `POST /api/orders/transaction`.
- **L72** — Méthode `transaction` : la commande vient du corps de la requête…
- **L73** — …et le paramètre d'URL `fail` (faux par défaut) demande un échec simulé.
- **L74** — Complète la commande reçue.
- **L75** — Ouvre un bloc `try`.
- **L76** — Envoie la commande et son audit dans une transaction Kafka.
- **L77** — En cas de succès, renvoie l'identifiant et l'état `COMMIT`.
- **L78** — Intercepte l'exception levée si la transaction a été annulée.
- **L79** — Renvoie l'identifiant, l'état `ABORT` et la cause de l'échec. `String.valueOf` évite une valeur nulle, que `Map.of` refuserait.
- **L80** — Ferme le bloc ouvert ligne 78.
- **L81** — Ferme le bloc ouvert ligne 73.
- **L82** — Ferme le bloc ouvert ligne 31.
