package com.bikestore.async.consumer;

import com.bikestore.async.config.RabbitMQConfig;
import com.bikestore.async.model.OrderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EmailWorker {

    private static final Logger log = LoggerFactory.getLogger(EmailWorker.class);

    private final ObjectMapper objectMapper;

    // Conjunto concurrente para control de idempotencia (evitar enviar correos duplicados)
    private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();

    public EmailWorker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.EMAILS_QUEUE)
    public void processEmail(String jsonPayload) throws Exception {
        OrderMessage order = objectMapper.readValue(jsonPayload, OrderMessage.class);

        // 1. Comprobar que el estado sea estrictamente PAID
        if (!"PAID".equalsIgnoreCase(order.paymentStatus())) {
            logEvent(
                    order.orderId(),
                    "EMAIL_SKIPPED",
                    "Correo omitido. El estado del pago no es PAID (Estado actual: " + order.paymentStatus() + ")"
            );
            return;
        }

        // 2. Control de duplicados (Idempotencia)
        if (!processedOrderIds.add(order.orderId())) {
            logEvent(
                    order.orderId(),
                    "EMAIL_DUPLICATE_IGNORED",
                    "Evento duplicado ignorado. Ya se envió previamente un correo para la orden " + order.orderId()
            );
            return;
        }

        // 3. Simulación del envío de correo mediante consola
        System.out.println("================================================================================");
        System.out.println("[SIMULACIÓN DE CORREO ELECTRÓNICO]");
        System.out.printf("Destinatario : %s (%s)%n", order.customerEmail(), order.customerName());
        System.out.printf("Asunto       : Confirmación de Pago - Pedido %s%n", order.orderId());
        System.out.printf("Monto Pagado : $%s%n", order.total());
        System.out.printf("Estado       : %s%n", order.paymentStatus());
        System.out.printf("Fecha/Hora   : %s%n", LocalDateTime.now());
        System.out.printf("Thread       : %s%n", Thread.currentThread().getName());
        System.out.println("Mensaje      : ¡Tu compra en BikeStore ha sido procesada con éxito!");
        System.out.println("================================================================================");

        // 4. Registro de log estructurado
        logEvent(
                order.orderId(),
                "EMAIL_SENT",
                "Correo de confirmación enviado exitosamente a " + order.customerEmail()
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
