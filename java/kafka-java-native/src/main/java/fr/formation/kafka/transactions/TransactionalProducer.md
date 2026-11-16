# `TransactionalProducer.java` — explication ligne à ligne

**Fichier** : `java/kafka-java-native/src/main/java/fr/formation/kafka/transactions/TransactionalProducer.java` · 59 lignes

Producteur transactionnel du TP03 : il écrit quatre messages dans deux topics de façon atomique, et valide ou annule la transaction selon l'argument reçu.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 13 · Package et imports

```java
 1  package fr.formation.kafka.transactions;
 2  
 3  import fr.formation.kafka.common.KafkaSettings;
 4  import org.apache.kafka.clients.producer.KafkaProducer;
 5  import org.apache.kafka.clients.producer.ProducerConfig;
 6  import org.apache.kafka.clients.producer.ProducerRecord;
 7  import org.apache.kafka.common.KafkaException;
 8  import org.apache.kafka.common.errors.AuthorizationException;
 9  import org.apache.kafka.common.errors.OutOfOrderSequenceException;
10  import org.apache.kafka.common.errors.ProducerFencedException;
11  import org.apache.kafka.common.serialization.StringSerializer;
12  
13  import java.util.Properties;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.transactions`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `KafkaSettings` : classe utilitaire du projet : adresse du cluster et propriétés de base.
- **L4** — Importe `KafkaProducer` : le client producteur Kafka (thread-safe).
- **L5** — Importe `ProducerConfig` : constantes des noms de paramètres du producteur.
- **L6** — Importe `ProducerRecord` : message à envoyer : topic, clé, valeur (et éventuellement partition, horodatage, en-têtes).
- **L7** — Importe `KafkaException` : classe mère des exceptions Kafka (non vérifiées).
- **L8** — Importe `AuthorizationException` : exception levée quand le client n'a pas les droits (ACL) nécessaires.
- **L9** — Importe `OutOfOrderSequenceException` : exception fatale du producteur idempotent : numéro de séquence incohérent.
- **L10** — Importe `ProducerFencedException` : exception fatale : un autre producteur de même `transactional.id` a pris la main (ce producteur est un « zombie »).
- **L11** — Importe `StringSerializer` : sérialiseur qui convertit une `String` en octets (UTF-8).
- **L13** — Importe `Properties` : table de propriétés clé → valeur, format attendu par les clients Kafka.

## Lignes 15 à 22 · Documentation et déclaration de la classe

```java
15  /**
16   * TP03 - Producer transactionnel : écriture ATOMIQUE dans plusieurs topics.
17   * <p>
18   * Les 3 messages de "transactions-in" et le message d'audit de "orders-audit"
19   * sont tous visibles... ou aucun ne l'est (pour un consumer en read_committed).
20   * Usage : ./run.sh transactions.TransactionalProducer [commit|abort]
21   */
22  public class TransactionalProducer {
```

- **L15–21** — Javadoc : écriture atomique dans plusieurs topics. Les 3 messages de `transactions-in` et le message d'audit de `orders-audit` sont tous visibles, ou aucun ne l'est, pour un consommateur en `read_committed`. Lancement : `./run.sh transactions.TransactionalProducer [commit|abort]`.
- **L22** — Déclare la classe publique `TransactionalProducer`.

## Lignes 24 à 25 · Point d'entrée et paramètre

```java
24      public static void main(String[] args) {
25          boolean commit = args.length == 0 || !"abort".equalsIgnoreCase(args[0]);
```

- **L24** — Méthode `main`.
- **L25** — `commit` vaut vrai s'il n'y a pas d'argument ou si l'argument n'est pas « abort » : par défaut, la transaction est validée.

## Lignes 27 à 31 · Configuration du producteur

```java
27          Properties props = KafkaSettings.base("transactional-producer");
28          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
29          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
30          // transactional.id : identifiant STABLE de l'instance (implique acks=all + idempotence)
31          props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "formation-tx-producer-1");
```

- **L27** — Propriétés de base avec `client.id=transactional-producer`.
- **L28** — Sérialiseur de la clé.
- **L29** — Sérialiseur de la valeur.
- **L30** — Commentaire : `transactional.id` est l'identifiant stable de l'instance ; il implique `acks=all` et l'idempotence.
- **L31** — `transactional.id` : rend le producteur transactionnel. Cet identifiant doit rester le même d'un redémarrage à l'autre : il permet au cluster d'écarter une ancienne instance « zombie » et de terminer une transaction restée ouverte.

## Lignes 33 à 34 · Initialisation

```java
33          try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
34              producer.initTransactions();     // enregistrement auprès du transaction coordinator
```

- **L33** — Crée le producteur, fermé automatiquement en fin de bloc.
- **L34** — `initTransactions()` : à appeler une seule fois ; le producteur s'enregistre auprès du coordinateur de transactions, qui annule au passage toute transaction inachevée du même identifiant.

## Lignes 35 à 48 · La transaction

```java
35              try {
36                  producer.beginTransaction();
37                  String txId = (commit ? "COMMIT-" : "ABORT-") + System.currentTimeMillis();
38                  for (int i = 1; i <= 3; i++) {
39                      producer.send(new ProducerRecord<>("transactions-in", "cle-" + i, txId + " ligne " + i));
40                  }
41                  producer.send(new ProducerRecord<>("orders-audit", txId, "audit de la transaction " + txId));
42                  if (commit) {
43                      producer.commitTransaction();
44                      System.out.println("Transaction COMMITEE : " + txId);
45                  } else {
46                      producer.abortTransaction();
47                      System.out.println("Transaction ANNULEE (abort) : " + txId);
48                  }
```

- **L35** — Ouvre le bloc `try` qui encadre la transaction.
- **L36** — Démarre une transaction.
- **L37** — Fabrique un identifiant lisible : préfixe `COMMIT-` ou `ABORT-` suivi de l'heure en millisecondes.
- **L38** — Boucle de 1 à 3.
- **L39** — Envoie trois messages dans le topic `transactions-in`.
- **L40** — Ferme le bloc ouvert ligne 38.
- **L41** — Envoie un message d'audit dans un autre topic, `orders-audit` : la transaction couvre plusieurs topics et partitions.
- **L42** — Selon l'argument reçu…
- **L43** — …valide la transaction : les quatre messages deviennent visibles en même temps pour les consommateurs en `read_committed`.
- **L44** — Affiche la confirmation.
- **L45** — Sinon…
- **L46** — …annule la transaction : les messages, bien qu'écrits dans les journaux, sont marqués comme annulés et ignorés par les consommateurs en `read_committed`.
- **L47** — Affiche l'annulation.
- **L48** — Ferme le bloc ouvert ligne 45.

## Lignes 49 à 59 · Gestion des erreurs

```java
49              } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
50                  // erreurs fatales : le producer ne peut plus être utilisé
51                  System.err.println("Erreur fatale : " + e.getMessage());
52              } catch (KafkaException e) {
53                  // erreur récupérable : on annule la transaction
54                  producer.abortTransaction();
55                  System.err.println("Transaction annulée suite à : " + e.getMessage());
56              }
57          }
58      }
59  }
```

- **L49** — Intercepte trois erreurs fatales : producteur évincé par une instance plus récente, incohérence de séquence, droits insuffisants.
- **L50** — Commentaire : après ces erreurs, le producteur ne peut plus être utilisé.
- **L51** — Affiche l'erreur ; on ne tente pas d'annuler, la seule issue est de fermer le producteur (ce que fait la sortie du bloc `try` externe).
- **L52** — Intercepte les autres exceptions Kafka.
- **L53** — Commentaire : ces erreurs sont récupérables.
- **L54** — Annule la transaction en cours.
- **L55** — Affiche la cause de l'annulation.
- **L56** — Ferme le bloc ouvert ligne 52.
- **L57** — Ferme le bloc ouvert ligne 33.
- **L58** — Ferme le bloc ouvert ligne 24.
- **L59** — Ferme le bloc ouvert ligne 22.
