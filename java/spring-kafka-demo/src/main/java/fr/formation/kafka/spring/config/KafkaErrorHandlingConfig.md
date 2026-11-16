# `KafkaErrorHandlingConfig.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/config/KafkaErrorHandlingConfig.java` · 25 lignes

Configuration de la gestion des erreurs des listeners : nouvelles tentatives, puis envoi du message en échec dans le *dead letter topic*.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 9 · Package et imports

```java
 1  package fr.formation.kafka.spring.config;
 2  
 3  import fr.formation.kafka.spring.model.Order;
 4  import org.springframework.context.annotation.Bean;
 5  import org.springframework.context.annotation.Configuration;
 6  import org.springframework.kafka.core.KafkaTemplate;
 7  import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
 8  import org.springframework.kafka.listener.DefaultErrorHandler;
 9  import org.springframework.util.backoff.FixedBackOff;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring.config`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `Order` : le *record* du projet représentant une commande.
- **L4** — Importe `Bean` : annotation : la méthode fournit un objet (bean) géré par Spring.
- **L5** — Importe `Configuration` : annotation : la classe déclare des beans.
- **L6** — Importe `KafkaTemplate` : façade Spring pour envoyer des messages.
- **L7** — Importe `DeadLetterPublishingRecoverer` : composant qui republie un message en échec dans un *dead letter topic*.
- **L8** — Importe `DefaultErrorHandler` : gestionnaire d'erreurs des listeners : nouvelles tentatives puis récupération.
- **L9** — Importe `FixedBackOff` : politique de nouvelles tentatives à intervalle fixe.

## Lignes 11 à 18 · Documentation et déclaration de la classe

```java
11  /**
12   * Gestion des erreurs des @KafkaListener : 2 nouvelles tentatives espacées d'1 s,
13   * puis publication du message en échec dans le Dead Letter Topic "orders-dlt"
14   * (suffixe "-dlt" par défaut depuis Spring for Apache Kafka 4.0).
15   * Spring Boot branche automatiquement ce bean CommonErrorHandler sur les listeners.
16   */
17  @Configuration
18  public class KafkaErrorHandlingConfig {
```

- **L11–16** — Javadoc : deux nouvelles tentatives espacées d'une seconde, puis publication du message en échec dans `orders-dlt` (suffixe `-dlt` par défaut depuis Spring for Apache Kafka 4.0). Spring Boot branche automatiquement ce bean sur les listeners.
- **L17** — `@Configuration` : la classe déclare des beans.
- **L18** — Déclare la classe publique `KafkaErrorHandlingConfig`.

## Lignes 20 à 25 · Le gestionnaire d'erreurs

```java
20      @Bean
21      DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Order> kafkaTemplate) {
22          DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
23          return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
24      }
25  }
```

- **L20** — `@Bean`.
- **L21** — Méthode fournissant le gestionnaire d'erreurs. Spring lui injecte le `KafkaTemplate` de l'application en paramètre.
- **L22** — Crée le « récupérateur » : quand les tentatives sont épuisées, il republie le message avec ce `KafkaTemplate` dans le topic `<topic d'origine>-dlt`, en ajoutant des en-têtes décrivant l'erreur.
- **L23** — Crée et renvoie le gestionnaire : `FixedBackOff(1000L, 2L)` signifie 1000 ms d'attente entre deux essais et 2 nouvelles tentatives au maximum, soit 3 essais en tout avant l'appel du récupérateur.
- **L24** — Ferme le bloc ouvert ligne 21.
- **L25** — Ferme le bloc ouvert ligne 18.
