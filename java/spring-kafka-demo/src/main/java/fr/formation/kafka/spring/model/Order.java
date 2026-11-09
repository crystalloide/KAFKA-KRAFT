package fr.formation.kafka.spring.model;

import java.util.UUID;

/**
 * Commande - même format JSON que les exemples Java natifs (OrderProducer).
 */
public record Order(String orderId, String customerId, String product,
                    int quantity, double unitPrice, String country, long ts) {

    /** Complète une commande reçue par l'API REST (identifiant, horodatage, pays). */
    public Order withDefaults() {
        return new Order(
                orderId == null || orderId.isBlank() ? "WEB-" + UUID.randomUUID().toString().substring(0, 8) : orderId,
                customerId,
                product,
                quantity,
                unitPrice,
                country == null || country.isBlank() ? "FR" : country,
                ts == 0 ? System.currentTimeMillis() : ts);
    }

    public double amount() {
        return quantity * unitPrice;
    }
}
