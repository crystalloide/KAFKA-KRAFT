package fr.formation.kafka.spring.web;

import fr.formation.kafka.spring.consumer.DltListener;
import fr.formation.kafka.spring.model.Order;
import fr.formation.kafka.spring.producer.OrderProducerService;
import fr.formation.kafka.spring.producer.TransactionalOrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
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
 *   <li>POST /api/orders?validate=true        : idem, mais refuse (HTTP 400) une quantité &lt;= 0</li>
 *   <li>POST /api/orders/random?count=20      : génère des commandes aléatoires</li>
 *   <li>POST /api/orders/transaction?fail=... : commande + audit dans une transaction</li>
 *   <li>GET  /api/orders/dlt                  : derniers messages du Dead Letter Topic</li>
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
    private final DltListener dltListener;

    public OrderController(OrderProducerService producer, TransactionalOrderService transactionalProducer,
                           DltListener dltListener) {
        this.producer = producer;
        this.transactionalProducer = transactionalProducer;
        this.dltListener = dltListener;
    }

    /**
     * Publie la commande dans le topic "orders" et répond dès que Kafka l'a acceptée (acks=all).
     * Le contrôle métier est fait PLUS TARD, de façon asynchrone, par le consumer OrderListener :
     * une commande invalide est donc acceptée ici (HTTP 200) puis part dans le Dead Letter Topic.
     * Avec ?validate=true, la quantité est contrôlée AVANT l'envoi : HTTP 400, rien n'est publié.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody Order request,
                                                      @RequestParam(defaultValue = "false") boolean validate)
            throws Exception {
        if (validate && request.quantity() <= 0) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("status", 400);
            error.put("error", "Quantité invalide : " + request.quantity());
            error.put("published", false);
            return ResponseEntity.badRequest().body(error);
        }
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
                    rnd.nextInt(1, 6), PRICES[p], COUNTRIES[rnd.nextInt(COUNTRIES.length)], 0L).withDefaults();
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

    /** Derniers messages arrivés dans "orders-dlt" (vus par DltListener), du plus ancien au plus récent. */
    @GetMapping("/dlt")
    public List<Map<String, Object>> deadLetters() {
        return dltListener.recent();
    }
}
