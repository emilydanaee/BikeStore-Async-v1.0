package com.bikestore.async.consumer;

import com.bikestore.async.config.RabbitMQConfig;
import com.bikestore.async.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PaymentWorker {

    private static final Logger log = LoggerFactory.getLogger(PaymentWorker.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    // Contador de intentos por orderId para registrar exactamente qué intento se está ejecutando (1, 2 o 3)
    private final ConcurrentHashMap<String, AtomicInteger> attemptCounter = new ConcurrentHashMap<>();

    public PaymentWorker(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.PAYMENTS_QUEUE)
    public void processPayment(String jsonPayload) throws Exception {
        OrderMessage order = objectMapper.readValue(jsonPayload, OrderMessage.class);

        // Incrementamos y obtenemos el número de intento actual
        int attempt = attemptCounter.computeIfAbsent(order.orderId(), k -> new AtomicInteger(0)).incrementAndGet();

        logEvent(
                order.orderId(),
                "PAYMENT_ATTEMPT_" + attempt,
                "Iniciando intento " + attempt + " de 3 para procesar el pago de $" + order.total()
        );

        // Determinación del resultado:
        // Si el correo contiene "fail" falla siempre; si contiene "success" aprueba siempre.
        // Para cualquier otro correo, aplica el 50% de probabilidad aleatoria.
        boolean isSuccess;
        if (order.customerEmail() != null && order.customerEmail().toLowerCase().contains("fail")) {
            isSuccess = false;
        } else if (order.customerEmail() != null && order.customerEmail().toLowerCase().contains("success")) {
            isSuccess = true;
        } else {
            isSuccess = random.nextBoolean(); // 50% de probabilidad
        }

        if (!isSuccess) {
            logEvent(
                    order.orderId(),
                    "PAYMENT_FAILED",
                    "Intento " + attempt + " de 3 fallido para el cliente " + order.customerEmail()
            );

            if (attempt >= 3) {
                attemptCounter.remove(order.orderId());
                logEvent(
                        order.orderId(),
                        "PAYMENT_DEAD_LETTERED",
                        "Se agotaron los 3 intentos. El mensaje será enviado a la Dead Letter Queue ("
                                + RabbitMQConfig.PAYMENTS_DLQ + ")"
                );
            }

            // Al lanzar excepción, Spring AMQP reintenta hasta el límite (3 intentos en total)
            // Cuando se agotan los 3 intentos, el mensaje es rechazado y enrutado a la DLQ
            throw new RuntimeException("Error simulado en la pasarela de pagos (Intento " + attempt + " de 3)");
        }

        // Si el pago es exitoso:
        attemptCounter.remove(order.orderId());

        logEvent(
                order.orderId(),
                "PAYMENT_SUCCESS",
                "Pago aprobado exitosamente en el intento " + attempt + ". Marcado como PAID."
        );

        // Generamos el mensaje con estado PAID
        OrderMessage paidOrder = new OrderMessage(
                order.orderId(),
                order.customerName(),
                order.customerEmail(),
                order.total(),
                "PAID",
                LocalDateTime.now()
        );

        String paidJson = objectMapper.writeValueAsString(paidOrder);

        // Publicamos el evento payment.completed para el servicio de correo
        rabbitTemplate.convertAndSend(
                RabbitMQConfig.ORDERS_EXCHANGE,
                RabbitMQConfig.PAYMENT_COMPLETED_ROUTING_KEY,
                paidJson
        );

        logEvent(
                order.orderId(),
                "PAYMENT_COMPLETED_EVENT_PUBLISHED",
                "Evento payment.completed enviado a RabbitMQ hacia " + RabbitMQConfig.EMAILS_QUEUE
        );
    }

    private void logEvent(String orderId, String event, String description) {
        log.info(
                "[{}] [{}] [thread={}] [{}] {}",
                orderId,
                LocalDateTime.now(),
                Thread.currentThread().getName(),
                event,
                description
        );
    }
}
