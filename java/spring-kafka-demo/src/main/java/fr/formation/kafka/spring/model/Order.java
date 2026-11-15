package fr.formation.kafka.spring.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public record Order(String orderId, String customerId, String product,
                    int quantity, double unitPrice, String country, Long ts) {

    @JsonCreator
    public Order(
            @JsonProperty("orderId") String orderId,
            @JsonProperty("customerId") String customerId,
            @JsonProperty("product") String product,
            @JsonProperty("quantity") int quantity,
            @JsonProperty("unitPrice") double unitPrice,
            @JsonProperty("country") String country,
            @JsonProperty("ts") Long ts) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.product = product;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.country = country;
        this.ts = ts;
    }

    /** Complète une commande reçue par l'API REST (identifiant, horodatage, pays). */
    public Order withDefaults() {
        return new Order(
                orderId == null || orderId.isBlank() ? "WEB-" + UUID.randomUUID().toString().substring(0, 8) : orderId,
                customerId,
                product,
                quantity,
                unitPrice,
                country == null || country.isBlank() ? "FR" : country,
                ts == null || ts == 0 ? System.currentTimeMillis() : ts);
    }

    public double amount() {
        return quantity * unitPrice;
    }
}
