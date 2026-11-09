package fr.formation.kafka.spring.producer;

import fr.formation.kafka.spring.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Envoi des commandes avec le KafkaTemplate auto-configuré par Spring Boot
 * (propriétés spring.kafka.producer.* de application.yml).
 */
@Service
public class OrderProducerService {

    public static final String TOPIC = "orders";
    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /** Envoi asynchrone : la clé customerId garantit l'ordre des commandes d'un même client. */
    public CompletableFuture<SendResult<String, Order>> send(Order order) {
        return kafkaTemplate.send(TOPIC, order.customerId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Echec de l'envoi de {} : {}", order.orderId(), ex.getMessage());
                    } else {
                        log.info("Commande {} envoyée -> partition {} offset {}", order.orderId(),
                                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }
}
