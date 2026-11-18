package fr.formation.kafka.spring.consumer;

import fr.formation.kafka.spring.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Supervision du Dead Letter Topic : affiche les messages en échec et la cause
 * (en-têtes kafka_dlt-* ajoutés par DeadLetterPublishingRecoverer).
 * Vue de supervision : à chaque démarrage, le DLT est relu depuis le début ; les 20 derniers
 * messages sont conservés en mémoire et exposés par GET /api/orders/dlt (voir OrderController).
 */
@Component
public class DltListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(DltListener.class);
    private static final int MAX_ENTRIES = 20;

    /** Clé "partition-offset" du message dans le DLT : pas de doublon si le DLT est relu. */
    private final Map<String, Map<String, Object>> recent = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Map<String, Object>> eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    /** Relit tout le DLT à chaque affectation de partitions (démarrage de l'application). */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(assignments.keySet());
    }

    @KafkaListener(id = "dlt-listener", topics = "orders-dlt", groupId = "order-dlt-monitor")
    public void onDeadLetter(ConsumerRecord<String, Order> record, Acknowledgment ack) {
        String orderId = record.value() == null ? "illisible" : record.value().orderId();
        String cause = rootMessage(header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
        String exception = simpleName(header(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN));
        String originalTopic = header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        Integer originalPartition = intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long originalOffset = longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);

        log.warn("DLT : commande {} en échec - cause : {} ({}) - origine : {}-{} offset {}",
                orderId, cause, exception, originalTopic, originalPartition, originalOffset);

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("orderId", orderId);
        entry.put("cause", cause);
        entry.put("exception", exception);
        entry.put("originalTopic", originalTopic);
        entry.put("originalPartition", originalPartition);
        entry.put("originalOffset", originalOffset);
        entry.put("dltPartition", record.partition());
        entry.put("dltOffset", record.offset());
        synchronized (recent) {
            recent.put(record.partition() + "-" + record.offset(), entry);
        }
        ack.acknowledge();
    }

    /** Derniers messages du DLT, du plus ancien au plus récent. */
    public List<Map<String, Object>> recent() {
        synchronized (recent) {
            return new ArrayList<>(recent.values());
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "-" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static Integer intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Integer.BYTES ? null : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value().length != Long.BYTES ? null : ByteBuffer.wrap(header.value()).getLong();
    }

    /** "Listener method '...' threw exception; Quantité invalide ..." -> "Quantité invalide ..." */
    private static String rootMessage(String message) {
        int i = message.lastIndexOf("; ");
        return i < 0 ? message : message.substring(i + 2);
    }

    private static String simpleName(String fqcn) {
        return fqcn.substring(fqcn.lastIndexOf('.') + 1);
    }
}
