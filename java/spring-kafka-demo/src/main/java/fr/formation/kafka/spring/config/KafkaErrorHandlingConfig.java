package fr.formation.kafka.spring.config;

import fr.formation.kafka.spring.model.Order;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Gestion des erreurs des @KafkaListener : 2 nouvelles tentatives espacées d'1 s,
 * puis publication du message en échec dans le Dead Letter Topic "orders-dlt"
 * (suffixe "-dlt" par défaut depuis Spring for Apache Kafka 4.0).
 * Spring Boot branche automatiquement ce bean CommonErrorHandler sur les listeners.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Order> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
    }
}
