package fr.formation.kafka.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Application de démonstration Spring for Apache Kafka.
 * Lancement : mvn spring-boot:run   (ou bouton "Run" dans IntelliJ)
 */
@SpringBootApplication
public class SpringKafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaDemoApplication.class, args);
    }
}
