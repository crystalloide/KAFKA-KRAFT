package fr.formation.kafka.spring.producer;

import fr.formation.kafka.spring.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Producer TRANSACTIONNEL : la commande (topic "orders") et sa ligne d'audit
 * (topic "orders-audit") sont écrites de façon atomique.
 * <p>
 * Le KafkaTemplate transactionnel est construit ici (et non déclaré comme bean) pour
 * conserver le KafkaTemplate non transactionnel auto-configuré par Spring Boot.
 */
@Service
public class TransactionalOrderService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(TransactionalOrderService.class);

    private final DefaultKafkaProducerFactory<String, Order> producerFactory;
    private final KafkaTemplate<String, Order> transactionalTemplate;

    public TransactionalOrderService(KafkaProperties kafkaProperties) {
        Map<String, Object> config = kafkaProperties.buildProducerProperties();
        this.producerFactory = new DefaultKafkaProducerFactory<>(config);
        this.producerFactory.setTransactionIdPrefix("order-tx-");
        this.transactionalTemplate = new KafkaTemplate<>(producerFactory);
    }

    /**
     * @param fail si true, une exception est levée APRES les envois : la transaction
     *             est annulée et aucun des deux messages n'est visible en read_committed.
     */
    public void sendWithAudit(Order order, boolean fail) {
        transactionalTemplate.executeInTransaction(operations -> {
            operations.send("orders", order.customerId(), order);
            operations.send("orders-audit", order.orderId(), order);
            if (fail) {
                throw new IllegalStateException("Echec simulé : la transaction est annulée");
            }
            return true;
        });
        log.info("Transaction commitée pour {}", order.orderId());
    }

    @Override
    public void destroy() {
        producerFactory.destroy();
    }
}
