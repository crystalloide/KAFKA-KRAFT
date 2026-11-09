package fr.formation.kafka.streams;

import fr.formation.kafka.common.KafkaSettings;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** Démarrage commun des applications Kafka Streams des TP. */
final class StreamsSupport {

    private StreamsSupport() {
    }

    /**
     * application.id = identifiant de l'application ; il sert de group.id, de préfixe
     * aux topics internes (changelog, repartition) et au répertoire d'état local.
     */
    static Properties config(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        // relit le topic d'entrée depuis le début au premier démarrage
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        return props;
    }

    static void run(StreamsBuilder builder, Properties props) {
        Topology topology = builder.build();
        System.out.println("Topologie :\n" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler(exception -> {
            System.err.println("Erreur fatale du traitement : " + exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.setStateListener((newState, oldState) ->
                System.out.println("Etat Kafka Streams : " + oldState + " -> " + newState));

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
