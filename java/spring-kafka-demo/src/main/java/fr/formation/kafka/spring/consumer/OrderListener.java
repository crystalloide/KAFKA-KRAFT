package fr.formation.kafka.spring.consumer;

import fr.formation.kafka.spring.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumer du groupe "order-service" : 3 threads (concurrency) se partagent les
 * 6 partitions du topic "orders". Commit manuel après traitement (ack-mode: manual).
 * Une commande de quantité <= 0 provoque une exception -> retries -> "orders-dlt".
 */
@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    @KafkaListener(id = "order-listener", topics = "orders", groupId = "order-service", concurrency = "3")
    public void onOrder(ConsumerRecord<String, Order> record, Acknowledgment ack) {
        Order order = record.value();
        log.info("Reçu {} (client {}, {} x {}) partition={} offset={}", order.orderId(), order.customerId(),
                order.quantity(), order.product(), record.partition(), record.offset());
        if (order.quantity() <= 0) {
            throw new IllegalArgumentException("Quantité invalide pour la commande " + order.orderId());
        }
        // ... traitement métier ...
        ack.acknowledge();
    }
}
