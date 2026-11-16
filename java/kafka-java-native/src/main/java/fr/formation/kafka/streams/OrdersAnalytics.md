# `OrdersAnalytics.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/streams/OrdersAnalytics.java` · 71 lignes

Cas métier Kafka Streams sur le topic `orders` : aiguillage des grosses commandes, chiffre d'affaires cumulé par pays et nombre de commandes par pays et par minute, avec tolérance aux messages illisibles.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 17 · Package et imports

```java
 1  package fr.formation.kafka.streams;
 2  
 3  import fr.formation.kafka.common.Order;
 4  import org.apache.kafka.common.serialization.Serdes;
 5  import org.apache.kafka.streams.KeyValue;
 6  import org.apache.kafka.streams.StreamsBuilder;
 7  import org.apache.kafka.streams.StreamsConfig;
 8  import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
 9  import org.apache.kafka.streams.kstream.Branched;
10  import org.apache.kafka.streams.kstream.Grouped;
11  import org.apache.kafka.streams.kstream.KStream;
12  import org.apache.kafka.streams.kstream.Materialized;
13  import org.apache.kafka.streams.kstream.Produced;
14  import org.apache.kafka.streams.kstream.TimeWindows;
15  
16  import java.time.Duration;
17  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.streams`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande (conversion JSON, lecture du CSV).
- **L4** — Importe `Serdes` : fabrique de *serdes* (sérialiseur + désérialiseur) pour les types courants.
- **L5** — Importe `KeyValue` : couple clé / valeur manipulé par les opérations du DSL.
- **L6** — Importe `StreamsBuilder` : point d'entrée du DSL : sert à décrire la topologie de traitement.
- **L7** — Importe `StreamsConfig` : constantes des noms de paramètres de Kafka Streams.
- **L8** — Importe `LogAndContinueProcessingExceptionHandler` : gestionnaire fourni par Kafka : journalise l'erreur de traitement puis passe au message suivant.
- **L9** — Importe `Branched` : décrit une branche d'un `split()` (nom, fonction ou consommateur appliqué à la branche).
- **L10** — Importe `Grouped` : précise les serdes à utiliser lors d'un regroupement (et du topic de repartition éventuel).
- **L11** — Importe `KStream` : flux d'enregistrements : chaque message est un fait indépendant.
- **L12** — Importe `Materialized` : précise comment matérialiser un état (nom du *state store*, serdes).
- **L13** — Importe `Produced` : précise comment écrire dans un topic de sortie (serdes de clé et de valeur).
- **L14** — Importe `TimeWindows` : définition de fenêtres temporelles de taille fixe.
- **L16** — Importe `Duration` : durée (utilisée pour les délais de `poll()` et les fenêtres).
- **L17** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 19 à 31 · Documentation et déclaration de la classe

```java
19  /**
20   * TP Kafka Streams 7 - cas métier sur le topic "orders" (JSON) :
21   * <ol>
22   *   <li>désérialisation JSON et filtrage des commandes invalides (quantité <= 0)</li>
23   *   <li>split() : les commandes >= 500 EUR vont dans "orders-big"</li>
24   *   <li>agrégat : chiffre d'affaires cumulé par pays -> "orders-revenue-by-country"</li>
25   *   <li>fenêtre : nombre de commandes par pays et par minute (fenêtres "tumbling")</li>
26   * </ol>
27   * Un message illisible (JSON invalide) est journalisé puis ignoré grâce au
28   * "processing.exception.handler" (KIP-1033) au lieu d'arrêter l'application.
29   * Usage : ./run.sh streams.OrdersAnalytics
30   */
31  public class OrdersAnalytics {
```

- **L19–30** — Javadoc : les quatre traitements (désérialisation JSON et filtrage des commandes invalides ; `split()` des commandes d'au moins 500 EUR vers `orders-big` ; chiffre d'affaires cumulé par pays vers `orders-revenue-by-country` ; comptage par pays sur des fenêtres d'une minute). Un message JSON invalide est journalisé puis ignoré grâce au `processing.exception.handler` (KIP-1033). Lancement par `./run.sh streams.OrdersAnalytics`.
- **L31** — Déclare la classe publique `OrdersAnalytics`.

## Lignes 33 à 38 · Flux des commandes valides

```java
33      public static void main(String[] args) {
34          StreamsBuilder builder = new StreamsBuilder();
35  
36          KStream<String, Order> orders = builder.<String, String>stream("orders")
37                  .mapValues(Order::fromJson)
38                  .filter((customer, order) -> order.quantity() > 0);
```

- **L33** — Méthode `main`.
- **L34** — Crée le `StreamsBuilder`.
- **L36** — Déclare le flux `orders` (clé `String`, valeur `Order`), construit à partir du topic `orders` lu en `String` / `String`.
- **L37** — `mapValues(Order::fromJson)` : chaque valeur JSON est convertie en objet `Order` par référence de méthode. Un JSON invalide lève une exception, prise en charge par le gestionnaire configuré plus bas.
- **L38** — `filter` : ne garde que les commandes dont la quantité est strictement positive.

## Lignes 40 à 47 · Traitement 1 : aiguillage des grosses commandes

```java
40          // 1) aiguillage des grosses commandes
41          orders.split()
42                  .branch((customer, order) -> order.amount() >= 500,
43                          Branched.withConsumer(big -> big
44                                  .peek((c, o) -> System.out.printf("GROSSE COMMANDE %s %.2f EUR%n", o.orderId(), o.amount()))
45                                  .mapValues(Order::toJson)
46                                  .to("orders-big")))
47                  .noDefaultBranch();
```

- **L40** — Commentaire de section.
- **L41** — `split()` : commence la séparation du flux en branches.
- **L42** — `branch` : première (et seule) branche, pour les commandes dont le montant atteint 500…
- **L43** — …traitée par `Branched.withConsumer`, qui reçoit le sous-flux `big` de cette branche.
- **L44** — `peek` : affiche l'identifiant et le montant de la grosse commande (deux décimales).
- **L45** — `mapValues(Order::toJson)` : reconvertit la commande en JSON.
- **L46** — Écrit le JSON dans le topic `orders-big`, avec les serdes par défaut (`String`). Les parenthèses ferment `withConsumer` et `branch`.
- **L47** — `noDefaultBranch()` : termine le `split` ; les commandes qui ne correspondent à aucune branche ne sont pas réémises ici (le flux `orders` d'origine reste utilisable pour les traitements suivants).

## Lignes 49 à 55 · Traitement 2 : chiffre d'affaires par pays

```java
49          // 2) chiffre d'affaires par pays (re-clé par pays => repartitionnement)
50          orders.map((customer, order) -> KeyValue.pair(order.country(), order.amount()))
51                  .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
52                  .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
53                  .toStream()
54                  .peek((country, total) -> System.out.printf("CA cumulé %s = %.2f EUR%n", country, total))
55                  .to("orders-revenue-by-country", Produced.with(Serdes.String(), Serdes.Double()));
```

- **L49** — Commentaire : changer la clé pour le pays impose un repartitionnement.
- **L50** — `map` : transforme chaque commande en couple (pays, montant). La clé change : Kafka Streams marque le flux comme « à repartitionner ».
- **L51** — `groupByKey` : regroupe par pays ; `Grouped.with` fournit les serdes (`String`, `Double`) du topic interne de repartition.
- **L52** — `reduce(Double::sum, …)` : additionne les montants de chaque pays. Le résultat est une `KTable` ; `Materialized.with` donne les serdes de son *state store*.
- **L53** — Convertit la table en flux de ses mises à jour.
- **L54** — `peek` : affiche le chiffre d'affaires cumulé du pays.
- **L55** — Écrit chaque mise à jour dans `orders-revenue-by-country`, clé `String` et valeur `Double`.

## Lignes 57 à 64 · Traitement 3 : commandes par pays et par minute

```java
57          // 3) nombre de commandes par pays sur des fenêtres d'1 minute
58          orders.map((customer, order) -> KeyValue.pair(order.country(), order.orderId()))
59                  .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
60                  .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
61                  .count()
62                  .toStream()
63                  .foreach((window, count) -> System.out.printf("fenêtre [%s - %s] %s : %d commande(s)%n",
64                          window.window().startTime(), window.window().endTime(), window.key(), count));
```

- **L57** — Commentaire de section.
- **L58** — `map` : transforme chaque commande en couple (pays, identifiant de commande).
- **L59** — `groupByKey` : regroupe par pays, avec les serdes `String` / `String`.
- **L60** — `windowedBy` : découpe le temps en fenêtres fixes d'une minute, sans recouvrement (*tumbling*) et sans période de grâce : un message arrivant après la fin de sa fenêtre est ignoré.
- **L61** — `count` : compte les commandes par pays et par fenêtre.
- **L62** — Convertit la table fenêtrée en flux ; la clé est de type `Windowed<String>` (pays + fenêtre).
- **L63** — `foreach` : opération terminale, affiche…
- **L64** — …le début et la fin de la fenêtre, le pays (`window.key()`) et le nombre de commandes.

## Lignes 66 à 71 · Configuration et démarrage

```java
66          Properties props = StreamsSupport.config("orders-analytics");
67          props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
68                  LogAndContinueProcessingExceptionHandler.class.getName());
69          StreamsSupport.run(builder, props);
70      }
71  }
```

- **L66** — Récupère la configuration commune avec l'identifiant d'application `orders-analytics`.
- **L67** — Définit le gestionnaire des exceptions de traitement…
- **L68** — …sur `LogAndContinueProcessingExceptionHandler` : l'erreur est journalisée et l'enregistrement fautif ignoré, au lieu d'arrêter l'application.
- **L69** — Démarre l'application et attend son arrêt.
- **L70** — Ferme le bloc ouvert ligne 33.
- **L71** — Ferme le bloc ouvert ligne 31.
