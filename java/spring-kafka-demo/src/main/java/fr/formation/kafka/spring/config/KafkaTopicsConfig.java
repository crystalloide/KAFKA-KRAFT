package fr.formation.kafka.spring.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Déclaration des topics : KafkaAdmin (auto-configuré par Spring Boot) les crée au
 * démarrage s'ils n'existent pas ("topics as code").
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name("orders").partitions(6).replicas(3)
                .config("min.insync.replicas", "2").build();
    }

    /** Dead Letter Topic : même nombre de partitions que "orders" (même partition réutilisée). */
    @Bean
    NewTopic ordersDltTopic() {
        return TopicBuilder.name("orders-dlt").partitions(6).replicas(3).build();
    }

    @Bean
    NewTopic ordersAuditTopic() {
        return TopicBuilder.name("orders-audit").partitions(3).replicas(3).build();
    }

    @Bean
    NewTopic ordersBigTopic() {
        return TopicBuilder.name("orders-big").partitions(3).replicas(3).build();
    }
}
