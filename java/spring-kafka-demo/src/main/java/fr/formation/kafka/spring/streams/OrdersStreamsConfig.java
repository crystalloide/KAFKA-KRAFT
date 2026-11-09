package fr.formation.kafka.spring.streams;

import fr.formation.kafka.spring.model.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

/**
 * Kafka Streams avec Spring (TP07b) : Spring Boot crée et pilote le StreamsBuilder
 * (application-id = spring.kafka.streams.application-id).
 * Activation : mvn spring-boot:run -Dspring-boot.run.arguments=--app.streams.enabled=true
 */
@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(name = "app.streams.enabled", havingValue = "true")
public class OrdersStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(OrdersStreamsConfig.class);

    @Bean
    KStream<String, Order> bigOrdersStream(StreamsBuilder builder) {
        JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);
        KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));
        orders.filter((customer, order) -> order != null && order.amount() >= 500)
                .peek((customer, order) -> log.info("[Streams] grosse commande {} : {} EUR",
                        order.orderId(), order.amount()))
                .to("orders-big", Produced.with(Serdes.String(), orderSerde));
        return orders;
    }
}
