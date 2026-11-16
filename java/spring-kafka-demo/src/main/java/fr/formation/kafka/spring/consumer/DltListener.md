# `DltListener.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/consumer/DltListener.java` · 37 lignes

Consommateur de supervision du *dead letter topic* : il journalise chaque message en échec avec la cause de l'erreur et son topic d'origine.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 13 · Package et imports

```java
 1  package fr.formation.kafka.spring.consumer;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.apache.kafka.clients.consumer.ConsumerRecord;
 5  import org.apache.kafka.common.header.Header;
 6  import org.slf4j.Logger;
 7  import org.slf4j.LoggerFactory;
 8  import org.springframework.kafka.annotation.KafkaListener;
 9  import org.springframework.kafka.support.Acknowledgment;
10  import org.springframework.kafka.support.KafkaHeaders;
11  import org.springframework.stereotype.Component;
12  
13  import java.nio.charset.StandardCharsets;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.consumer`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `ConsumerRecord` : un message lu : topic, partition, offset, clé, valeur, horodatage, en-têtes.
- **L5** — Importe `Header` : un en-tête de message : nom + valeur en octets.
- **L6** — Importe `Logger` : interface de journalisation SLF4J.
- **L7** — Importe `LoggerFactory` : fabrique des journaliseurs SLF4J.
- **L8** — Importe `KafkaListener` : annotation : la méthode est appelée pour chaque message reçu d'un topic.
- **L9** — Importe `Acknowledgment` : objet permettant au listener de valider (commit) manuellement l'offset.
- **L10** — Importe `KafkaHeaders` : constantes des noms d'en-têtes utilisés par Spring for Apache Kafka.
- **L11** — Importe `Component` : annotation : la classe est un composant instancié et géré par Spring.
- **L13** — Importe `StandardCharsets` : constantes des jeux de caractères standard (UTF-8…).

## Lignes 15 à 22 · Documentation et déclaration de la classe

```java
15  /**
16   * Supervision du Dead Letter Topic : affiche les messages en échec et la cause
17   * (en-têtes kafka_dlt-* ajoutés par DeadLetterPublishingRecoverer).
18   */
19  @Component
20  public class DltListener {
21  
22      private static final Logger log = LoggerFactory.getLogger(DltListener.class);
```

- **L15–18** — Javadoc : supervision du *dead letter topic* ; affiche les messages en échec et leur cause, lue dans les en-têtes `kafka_dlt-*` ajoutés par `DeadLetterPublishingRecoverer`.
- **L19** — `@Component` : composant Spring.
- **L20** — Déclare la classe publique `DltListener`.
- **L22** — Journaliseur de la classe.

## Lignes 24 à 31 · Le listener

```java
24      @KafkaListener(id = "dlt-listener", topics = "orders-dlt", groupId = "order-dlt-monitor")
25      public void onDeadLetter(ConsumerRecord<String, Order> record, Acknowledgment ack) {
26          log.warn("DLT : commande {} en échec - cause : {} (topic d'origine : {})",
27                  record.value() == null ? "illisible" : record.value().orderId(),
28                  header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
29                  header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
30          ack.acknowledge();
31      }
```

- **L24** — `@KafkaListener` : conteneur `dlt-listener`, abonné au topic `orders-dlt`, dans un groupe distinct `order-dlt-monitor`.
- **L25** — Méthode appelée pour chaque message du *dead letter topic*.
- **L26** — Journalise un avertissement à trois emplacements variables…
- **L27** — …l'identifiant de la commande, ou « illisible » si la valeur n'a pas pu être désérialisée…
- **L28** — …le message de l'exception d'origine, lu dans l'en-tête prévu à cet effet…
- **L29** — …et le topic d'origine du message.
- **L30** — Valide l'offset : le message en échec a été pris en compte.
- **L31** — Ferme le bloc ouvert ligne 25.

## Lignes 33 à 37 · Méthode utilitaire header()

```java
33      private static String header(ConsumerRecord<?, ?> record, String name) {
34          Header header = record.headers().lastHeader(name);
35          return header == null ? "-" : new String(header.value(), StandardCharsets.UTF_8);
36      }
37  }
```

- **L33** — Méthode privée qui renvoie la valeur d'un en-tête sous forme de texte. Les `?` acceptent un `ConsumerRecord` de n'importe quels types.
- **L34** — Lit le dernier en-tête portant ce nom (il peut y en avoir plusieurs).
- **L35** — S'il est absent, renvoie un tiret ; sinon convertit ses octets en chaîne UTF-8.
- **L36** — Ferme le bloc ouvert ligne 33.
- **L37** — Ferme le bloc ouvert ligne 20.
