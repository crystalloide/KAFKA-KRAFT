package fr.formation.kafka.spring.web;

import fr.formation.kafka.spring.model.Order;
import fr.formation.kafka.spring.producer.OrderProducerService;
import fr.formation.kafka.spring.producer.TransactionalOrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * API REST :
 * <ul>
 *   <li>POST /api/orders                      : envoie la commande reçue en JSON</li>
 *   <li>POST /api/orders/random?count=20      : génère des commandes aléatoires</li>
 *   <li>POST /api/orders/transaction?fail=... : commande + audit dans une transaction</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final String[] PRODUCTS = {"Clavier", "Souris", "Ecran 27p", "Casque", "Laptop", "Dock USB-C"};
    private static final double[] PRICES = {49.9, 19.9, 289.0, 89.0, 1299.0, 129.0};
    private static final String[] COUNTRIES = {"FR", "BE", "CH", "CA", "DE"};

    private final OrderProducerService producer;
    private final TransactionalOrderService transactionalProducer;

    public OrderController(OrderProducerService producer, TransactionalOrderService transactionalProducer) {
        this.producer = producer;
        this.transactionalProducer = transactionalProducer;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody Order request) throws Exception {
        Order order = request.withDefaults();
        SendResult<String, Order> result = producer.send(order).get(10, TimeUnit.SECONDS);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("orderId", order.orderId());
        body.put("topic", result.getRecordMetadata().topic());
        body.put("partition", result.getRecordMetadata().partition());
        body.put("offset", result.getRecordMetadata().offset());
        return ResponseEntity.ok(body);
    }

    @PostMapping("/random")
    public Map<String, Object> random(@RequestParam(defaultValue = "10") int count) {
        List<String> ids = new ArrayList<>();
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            int p = rnd.nextInt(PRODUCTS.length);
            Order order = new Order(null, "C-%03d".formatted(rnd.nextInt(1, 11)), PRODUCTS[p],
                    rnd.nextInt(1, 6), PRICES[p], COUNTRIES[rnd.nextInt(COUNTRIES.length)], 0).withDefaults();
            producer.send(order);
            ids.add(order.orderId());
        }
        return Map.of("sent", count, "orderIds", ids);
    }

    @PostMapping("/transaction")
    public Map<String, Object> transaction(@RequestBody Order request,
                                           @RequestParam(defaultValue = "false") boolean fail) {
        Order order = request.withDefaults();
        try {
            transactionalProducer.sendWithAudit(order, fail);
            return Map.of("orderId", order.orderId(), "transaction", "COMMIT");
        } catch (RuntimeException e) {
            return Map.of("orderId", order.orderId(), "transaction", "ABORT", "cause", String.valueOf(e.getMessage()));
        }
    }
}
