package fr.formation.kafka.common;

import org.apache.kafka.clients.CommonClientConfigs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Paramètres communs à tous les exemples.
 * <p>
 * L'adresse du cluster peut être surchargée par la variable d'environnement
 * KAFKA_BOOTSTRAP_SERVERS (utile si l'IDE tourne sous Windows ou pour viser
 * le noeud "standalone" du TP01 : localhost:9192).
 */
public final class KafkaSettings {

    public static final String DEFAULT_BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096";

    private KafkaSettings() {
    }

    public static String bootstrapServers() {
        String env = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return (env == null || env.isBlank()) ? DEFAULT_BOOTSTRAP : env;
    }

    /** Propriétés de base : adresse du cluster et identifiant du client. */
    public static Properties base(String clientId) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        return props;
    }

    /**
     * Retrouve le fichier data/orders.csv du dépôt, que le programme soit lancé
     * depuis java/kafka-java-native (run.sh) ou depuis la racine du dépôt (IDE).
     */
    public static Path dataFile(String name) {
        for (String candidate : new String[]{"data/" + name, "../../data/" + name, "../data/" + name}) {
            Path path = Path.of(candidate);
            if (Files.exists(path)) {
                return path;
            }
        }
        throw new IllegalStateException("Fichier data/" + name + " introuvable depuis " + Path.of("").toAbsolutePath());
    }
}
