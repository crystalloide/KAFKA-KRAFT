package fr.formation.kafka.spring.consumer;

import fr.formation.kafka.spring.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Supervision du Dead Letter Topic : affiche les messages en échec et la cause
 * (en-têtes kafka_dlt-* ajoutés par DeadLetterPublishingRecoverer).
 */
@Component
public class DltListener {

    private static final Logger log = LoggerFactory.getLogger(DltListener.class);

    @KafkaListener(id = "dlt-listener", topics = "orders-dlt", groupId = "order-dlt-monitor")
    public void onDeadLetter(ConsumerRecord<String, Order> record, Acknowledgment ack) {
        log.warn("DLT : commande {} en échec - cause : {} (topic d'origine : {})",
                record.value() == null ? "illisible" : record.value().orderId(),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
        ack.acknowledge();
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "-" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
