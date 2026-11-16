# `Order.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/model/Order.java` · 43 lignes

Modèle de données de l'application Spring Boot : une commande, sous forme de *record*, annotée pour la désérialisation JSON et complétée de valeurs par défaut.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 5 · Package et imports

```java
 1  package fr.formation.kafka.spring.model;
 2  
 3  import com.fasterxml.jackson.annotation.JsonCreator;
 4  import com.fasterxml.jackson.annotation.JsonProperty;
 5  import java.util.UUID;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.model`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `JsonCreator` : annotation Jackson : désigne le constructeur à utiliser pour désérialiser.
- **L4** — Importe `JsonProperty` : annotation Jackson : associe un paramètre ou un champ à un nom de propriété JSON.
- **L5** — Importe `UUID` : identifiant unique universel.

## Lignes 7 à 8 · Déclaration du record

```java
 7  public record Order(String orderId, String customerId, String product,
 8                      int quantity, double unitPrice, String country, Long ts) {
```

- **L7–8** — Déclare le *record* `Order` et ses sept composants. L'horodatage `ts` est de type `Long` (objet) et non `long` : il peut ainsi être absent (`null`) dans le JSON reçu par l'API REST.

## Lignes 10 à 26 · Constructeur annoté pour Jackson

```java
10      @JsonCreator
11      public Order(
12              @JsonProperty("orderId") String orderId,
13              @JsonProperty("customerId") String customerId,
14              @JsonProperty("product") String product,
15              @JsonProperty("quantity") int quantity,
16              @JsonProperty("unitPrice") double unitPrice,
17              @JsonProperty("country") String country,
18              @JsonProperty("ts") Long ts) {
19          this.orderId = orderId;
20          this.customerId = customerId;
21          this.product = product;
22          this.quantity = quantity;
23          this.unitPrice = unitPrice;
24          this.country = country;
25          this.ts = ts;
26      }
```

- **L10** — `@JsonCreator` : indique à Jackson d'utiliser ce constructeur pour créer l'objet à partir du JSON.
- **L11** — Début du constructeur canonique du record, écrit explicitement pour pouvoir annoter ses paramètres.
- **L12** — `@JsonProperty("orderId")` : la propriété JSON `orderId` alimente ce paramètre.
- **L13** — Même principe pour `customerId`.
- **L14** — Même principe pour `product`.
- **L15** — Même principe pour `quantity`.
- **L16** — Même principe pour `unitPrice`.
- **L17** — Même principe pour `country`.
- **L18** — Même principe pour `ts` ; fin de la liste des paramètres et début du corps du constructeur.
- **L19** — Affecte le champ `orderId`.
- **L20** — Affecte le champ `customerId`.
- **L21** — Affecte le champ `product`.
- **L22** — Affecte le champ `quantity`.
- **L23** — Affecte le champ `unitPrice`.
- **L24** — Affecte le champ `country`.
- **L25** — Affecte le champ `ts`.
- **L26** — Ferme le bloc ouvert ligne 18.

## Lignes 28 à 38 · Méthode withDefaults() : compléter une commande

```java
28      /** Complète une commande reçue par l'API REST (identifiant, horodatage, pays). */
29      public Order withDefaults() {
30          return new Order(
31                  orderId == null || orderId.isBlank() ? "WEB-" + UUID.randomUUID().toString().substring(0, 8) : orderId,
32                  customerId,
33                  product,
34                  quantity,
35                  unitPrice,
36                  country == null || country.isBlank() ? "FR" : country,
37                  ts == null || ts == 0 ? System.currentTimeMillis() : ts);
38      }
```

- **L28** — Javadoc : complète une commande reçue par l'API REST (identifiant, horodatage, pays).
- **L29** — Déclare la méthode `withDefaults()`.
- **L30** — Un record n'étant pas modifiable, on renvoie une nouvelle commande.
- **L31** — Identifiant : s'il est absent ou vide, on en fabrique un (`WEB-` suivi des 8 premiers caractères d'un UUID aléatoire) ; sinon on garde celui reçu.
- **L32** — Client : inchangé.
- **L33** — Produit : inchangé.
- **L34** — Quantité : inchangée.
- **L35** — Prix unitaire : inchangé.
- **L36** — Pays : `FR` s'il est absent ou vide.
- **L37** — Horodatage : l'heure courante s'il est absent ou nul.
- **L38** — Ferme le bloc ouvert ligne 29.

## Lignes 40 à 43 · Méthode amount()

```java
40      public double amount() {
41          return quantity * unitPrice;
42      }
43  }
```

- **L40** — Déclare la méthode `amount()`.
- **L41** — Renvoie le montant de la commande : quantité × prix unitaire.
- **L42** — Ferme le bloc ouvert ligne 40.
- **L43** — Ferme le bloc ouvert ligne 8.
