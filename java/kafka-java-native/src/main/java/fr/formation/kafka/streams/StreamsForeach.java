package fr.formation.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;

/**
 * TP Kafka Streams 5 - opérations terminales : peek (observation sans modifier le flux)
 * et foreach (action finale, aucun topic de sortie).
 * Usage : ./run.sh streams.StreamsForeach
 */
public class StreamsForeach {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream("streams-input")
                .peek((key, value) -> System.out.println("peek    : reçu " + key + " -> " + value))
                .filter((key, value) -> value != null && value.length() > 3)
                .foreach((key, value) -> System.out.printf("foreach : [%s] longueur=%d%n", value, value.length()));
        StreamsSupport.run(builder, StreamsSupport.config("streams-foreach"));
    }
}
