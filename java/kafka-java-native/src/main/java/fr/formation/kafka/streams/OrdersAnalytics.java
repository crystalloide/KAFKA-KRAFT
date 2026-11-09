package fr.formation.kafka.streams;

import fr.formation.kafka.common.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.Properties;

/**
 * TP Kafka Streams 7 - cas métier sur le topic "orders" (JSON) :
 * <ol>
 *   <li>désérialisation JSON et filtrage des commandes invalides (quantité <= 0)</li>
 *   <li>split() : les commandes >= 500 EUR vont dans "orders-big"</li>
 *   <li>agrégat : chiffre d'affaires cumulé par pays -> "orders-revenue-by-country"</li>
 *   <li>fenêtre : nombre de commandes par pays et par minute (fenêtres "tumbling")</li>
 * </ol>
 * Un message illisible (JSON invalide) est journalisé puis ignoré grâce au
 * "processing.exception.handler" (KIP-1033) au lieu d'arrêter l'application.
 * Usage : ./run.sh streams.OrdersAnalytics
 */
public class OrdersAnalytics {

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Order> orders = builder.<String, String>stream("orders")
                .mapValues(Order::fromJson)
                .filter((customer, order) -> order.quantity() > 0);

        // 1) aiguillage des grosses commandes
        orders.split()
                .branch((customer, order) -> order.amount() >= 500,
                        Branched.withConsumer(big -> big
                                .peek((c, o) -> System.out.printf("GROSSE COMMANDE %s %.2f EUR%n", o.orderId(), o.amount()))
                                .mapValues(Order::toJson)
                                .to("orders-big")))
                .noDefaultBranch();

        // 2) chiffre d'affaires par pays (re-clé par pays => repartitionnement)
        orders.map((customer, order) -> KeyValue.pair(order.country(), order.amount()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((country, total) -> System.out.printf("CA cumulé %s = %.2f EUR%n", country, total))
                .to("orders-revenue-by-country", Produced.with(Serdes.String(), Serdes.Double()));

        // 3) nombre de commandes par pays sur des fenêtres d'1 minute
        orders.map((customer, order) -> KeyValue.pair(order.country(), order.orderId()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count()
                .toStream()
                .foreach((window, count) -> System.out.printf("fenêtre [%s - %s] %s : %d commande(s)%n",
                        window.window().startTime(), window.window().endTime(), window.key(), count));

        Properties props = StreamsSupport.config("orders-analytics");
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueProcessingExceptionHandler.class.getName());
        StreamsSupport.run(builder, props);
    }
}
