# `Order.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/common/Order.java` · 61 lignes

Modèle de données des exemples Java natifs : une commande, sous forme de *record* Java, avec sa conversion vers et depuis JSON et le chargement du jeu de données CSV.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 12 · Package et imports

```java
 1  package fr.formation.kafka.common;
 2  
 3  import com.fasterxml.jackson.annotation.JsonIgnore;
 4  import com.fasterxml.jackson.core.JsonProcessingException;
 5  import com.fasterxml.jackson.databind.DeserializationFeature;
 6  import com.fasterxml.jackson.databind.ObjectMapper;
 7  
 8  import java.io.IOException;
 9  import java.io.UncheckedIOException;
10  import java.nio.file.Files;
11  import java.nio.file.Path;
12  import java.util.List;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.common`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `JsonIgnore` : annotation Jackson : exclut une propriété de la sérialisation JSON.
- **L4** — Importe `JsonProcessingException` : exception Jackson levée en cas d'erreur de lecture ou d'écriture JSON.
- **L5** — Importe `DeserializationFeature` : options de désérialisation de Jackson.
- **L6** — Importe `ObjectMapper` : objet central de Jackson : convertit objets Java ↔ JSON.
- **L8** — Importe `IOException` : exception vérifiée des entrées / sorties.
- **L9** — Importe `UncheckedIOException` : enveloppe non vérifiée d'une `IOException`.
- **L10** — Importe `Files` : méthodes utilitaires sur les fichiers (existence, lecture…).
- **L11** — Importe `Path` : chemin de fichier.
- **L12** — Importe `List` : interface des listes.

## Lignes 14 à 20 · Documentation et déclaration du record

```java
14  /**
15   * Commande (même format JSON que l'application Spring Boot du TP).
16   * Exemple : {"orderId":"ORD-0001","customerId":"C-005","product":"Souris",
17   *            "quantity":1,"unitPrice":19.9,"country":"BE","ts":1731160000000}
18   */
19  public record Order(String orderId, String customerId, String product,
20                      int quantity, double unitPrice, String country, long ts) {
```

- **L14–18** — Javadoc : une commande, au même format JSON que l'application Spring Boot du TP, avec un exemple de message.
- **L19–20** — Déclare le *record* `Order` et ses sept composants : identifiant de commande, identifiant client, produit, quantité, prix unitaire, pays et horodatage (`ts`, en millisecondes). Un record génère automatiquement le constructeur, les accesseurs (`orderId()`, `quantity()`…), `equals`, `hashCode` et `toString`.

## Lignes 22 à 23 · L'ObjectMapper partagé

```java
22      private static final ObjectMapper MAPPER = new ObjectMapper()
23              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
```

- **L22** — Crée un unique `ObjectMapper` Jackson, statique et partagé (sa création est coûteuse et il est thread-safe).
- **L23** — Le configure pour ignorer les propriétés JSON inconnues : un message contenant un champ supplémentaire reste lisible, ce qui facilite l'évolution du format.

## Lignes 25 à 28 · Méthode amount() : montant de la commande

```java
25      @JsonIgnore
26      public double amount() {
27          return quantity * unitPrice;
28      }
```

- **L25** — `@JsonIgnore` : le montant calculé ne doit pas apparaître dans le JSON produit (sinon Jackson sérialiserait cette méthode d'accès comme une propriété).
- **L26** — Déclare la méthode `amount()`.
- **L27** — Renvoie quantité × prix unitaire.
- **L28** — Ferme le bloc ouvert ligne 26.

## Lignes 30 à 36 · Méthode toJson() : objet → JSON

```java
30      public String toJson() {
31          try {
32              return MAPPER.writeValueAsString(this);
33          } catch (JsonProcessingException e) {
34              throw new IllegalStateException(e);
35          }
36      }
```

- **L30** — Déclare la méthode qui convertit la commande en chaîne JSON.
- **L31** — Ouvre un bloc `try` car Jackson peut lever une exception vérifiée.
- **L32** — Sérialise l'objet courant (`this`) en JSON.
- **L33** — Intercepte l'erreur de sérialisation éventuelle.
- **L34** — La relance sous forme d'exception non vérifiée (`IllegalStateException`) : ce cas ne devrait jamais arriver avec un record aussi simple.
- **L35** — Ferme le bloc ouvert ligne 33.
- **L36** — Ferme le bloc ouvert ligne 30.

## Lignes 38 à 44 · Méthode fromJson() : JSON → objet

```java
38      public static Order fromJson(String json) {
39          try {
40              return MAPPER.readValue(json, Order.class);
41          } catch (JsonProcessingException e) {
42              throw new IllegalArgumentException("JSON de commande invalide : " + json, e);
43          }
44      }
```

- **L38** — Déclare la méthode statique qui construit une commande à partir d'une chaîne JSON.
- **L39** — Ouvre le bloc `try`.
- **L40** — Désérialise le JSON en instance de `Order`.
- **L41** — Intercepte l'erreur de lecture (JSON mal formé ou types incompatibles).
- **L42** — Lève une `IllegalArgumentException` contenant le JSON fautif et la cause d'origine. C'est cette exception que Kafka Streams interceptera dans `OrdersAnalytics`.
- **L43** — Ferme le bloc ouvert ligne 41.
- **L44** — Ferme le bloc ouvert ligne 38.

## Lignes 46 à 61 · Méthode loadCsv() : chargement du jeu de données

```java
46      /** Charge le jeu de données CSV : orderId,customerId,product,quantity,unitPrice,country */
47      public static List<Order> loadCsv(Path csv) {
48          try {
49              long now = System.currentTimeMillis();
50              return Files.readAllLines(csv).stream()
51                      .skip(1)
52                      .filter(line -> !line.isBlank())
53                      .map(line -> line.split(","))
54                      .map(f -> new Order(f[0], f[1], f[2], Integer.parseInt(f[3]),
55                              Double.parseDouble(f[4]), f[5], now))
56                      .toList();
57          } catch (IOException e) {
58              throw new UncheckedIOException(e);
59          }
60      }
61  }
```

- **L46** — Javadoc : charge le fichier CSV dont les colonnes sont `orderId,customerId,product,quantity,unitPrice,country`.
- **L47** — Déclare la méthode statique `loadCsv`, qui reçoit le chemin du fichier et renvoie une liste de commandes.
- **L48** — Ouvre le bloc `try` (la lecture du fichier peut échouer).
- **L49** — Mémorise l'heure courante : elle servira d'horodatage à toutes les commandes chargées.
- **L50** — Lit toutes les lignes du fichier et les transforme en flux Java (`Stream`).
- **L51** — Saute la première ligne, qui contient les en-têtes de colonnes.
- **L52** — Écarte les lignes vides.
- **L53** — Découpe chaque ligne sur les virgules : on obtient un tableau de champs.
- **L54–55** — Construit une commande à partir des champs, en convertissant la quantité en entier et le prix en nombre décimal ; l'horodatage est `now`.
- **L56** — Rassemble les commandes dans une liste non modifiable.
- **L57** — Intercepte l'erreur d'entrée / sortie (fichier absent ou illisible).
- **L58** — La relance sous forme non vérifiée (`UncheckedIOException`) pour ne pas imposer de `throws` aux appelants.
- **L59** — Ferme le bloc ouvert ligne 57.
- **L60** — Ferme le bloc ouvert ligne 47.
- **L61** — Ferme le bloc ouvert ligne 20.
