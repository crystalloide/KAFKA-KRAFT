package fr.formation.kafka.transactions;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * TP03 - Producer transactionnel : écriture ATOMIQUE dans plusieurs topics.
 * <p>
 * Les 3 messages de "transactions-in" et le message d'audit de "orders-audit"
 * sont tous visibles... ou aucun ne l'est (pour un consumer en read_committed).
 * Usage : ./run.sh transactions.TransactionalProducer [commit|abort]
 */
public class TransactionalProducer {

    public static void main(String[] args) {
        boolean commit = args.length == 0 || !"abort".equalsIgnoreCase(args[0]);

        Properties props = KafkaSettings.base("transactional-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // transactional.id : identifiant STABLE de l'instance (implique acks=all + idempotence)
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "formation-tx-producer-1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions();     // enregistrement auprès du transaction coordinator
            try {
                producer.beginTransaction();
                String txId = (commit ? "COMMIT-" : "ABORT-") + System.currentTimeMillis();
                for (int i = 1; i <= 3; i++) {
                    producer.send(new ProducerRecord<>("transactions-in", "cle-" + i, txId + " ligne " + i));
                }
                producer.send(new ProducerRecord<>("orders-audit", txId, "audit de la transaction " + txId));
                if (commit) {
                    producer.commitTransaction();
                    System.out.println("Transaction COMMITEE : " + txId);
                } else {
                    producer.abortTransaction();
                    System.out.println("Transaction ANNULEE (abort) : " + txId);
                }
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // erreurs fatales : le producer ne peut plus être utilisé
                System.err.println("Erreur fatale : " + e.getMessage());
            } catch (KafkaException e) {
                // erreur récupérable : on annule la transaction
                producer.abortTransaction();
                System.err.println("Transaction annulée suite à : " + e.getMessage());
            }
        }
    }
}
