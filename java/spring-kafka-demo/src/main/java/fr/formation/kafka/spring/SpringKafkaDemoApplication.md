# `SpringKafkaDemoApplication.java` — explication ligne à ligne

**Fichier** : `java/spring-kafka-demo/src/main/java/fr/formation/kafka/spring/SpringKafkaDemoApplication.java` · 16 lignes

Classe de démarrage de l'application Spring Boot de démonstration.

> Les numéros de ligne renvoient au fichier source. Les lignes vides ne sont pas commentées.

## Lignes 1 à 4 · Package et imports

```java
 1  package fr.formation.kafka.spring;
 2  
 3  import org.springframework.boot.SpringApplication;
 4  import org.springframework.boot.autoconfigure.SpringBootApplication;
```

- **L1** — Déclare le package de la classe : `fr.formation.kafka.spring`. Il correspond à l'arborescence de répertoires du fichier.
- **L3** — Importe `SpringApplication` : classe qui démarre une application Spring Boot.
- **L4** — Importe `SpringBootApplication` : annotation qui active l'auto-configuration et la recherche des composants.

## Lignes 6 à 11 · Documentation et déclaration de la classe

```java
 6  /**
 7   * Application de démonstration Spring for Apache Kafka.
 8   * Lancement : mvn spring-boot:run   (ou bouton "Run" dans IntelliJ)
 9   */
10  @SpringBootApplication
11  public class SpringKafkaDemoApplication {
```

- **L6–9** — Javadoc : application de démonstration Spring for Apache Kafka ; lancement par `mvn spring-boot:run` ou par le bouton « Run » d'IntelliJ.
- **L10** — `@SpringBootApplication` : active l'auto-configuration de Spring Boot (dont celle de Kafka, à partir de `application.yml`) et la recherche des composants dans ce package et ses sous-packages.
- **L11** — Déclare la classe publique `SpringKafkaDemoApplication`.

## Lignes 13 à 16 · Point d'entrée

```java
13      public static void main(String[] args) {
14          SpringApplication.run(SpringKafkaDemoApplication.class, args);
15      }
16  }
```

- **L13** — Méthode `main`, point d'entrée de la JVM.
- **L14** — Démarre Spring Boot : création du contexte, des beans (topics, `KafkaTemplate`, listeners…) et du serveur web embarqué. Les arguments de la ligne de commande sont transmis à Spring.
- **L15** — Ferme le bloc ouvert ligne 13.
- **L16** — Ferme le bloc ouvert ligne 11.
