package fr.formation.kafka.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Commande (même format JSON que l'application Spring Boot du TP).
 * Exemple : {"orderId":"ORD-0001","customerId":"C-005","product":"Souris",
 *            "quantity":1,"unitPrice":19.9,"country":"BE","ts":1731160000000}
 */
public record Order(String orderId, String customerId, String product,
                    int quantity, double unitPrice, String country, long ts) {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @JsonIgnore
    public double amount() {
        return quantity * unitPrice;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Order fromJson(String json) {
        try {
            return MAPPER.readValue(json, Order.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON de commande invalide : " + json, e);
        }
    }

    /** Charge le jeu de données CSV : orderId,customerId,product,quantity,unitPrice,country */
    public static List<Order> loadCsv(Path csv) {
        try {
            long now = System.currentTimeMillis();
            return Files.readAllLines(csv).stream()
                    .skip(1)
                    .filter(line -> !line.isBlank())
                    .map(line -> line.split(","))
                    .map(f -> new Order(f[0], f[1], f[2], Integer.parseInt(f[3]),
                            Double.parseDouble(f[4]), f[5], now))
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
