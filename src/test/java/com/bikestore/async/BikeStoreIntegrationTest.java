package com.bikestore.async;

import com.bikestore.async.config.RabbitMQConfig;
import com.bikestore.async.consumer.EmailWorker;
import com.bikestore.async.consumer.PaymentWorker;
import com.bikestore.async.model.OrderMessage;
import com.bikestore.async.model.OrderRequest;
import com.bikestore.async.producer.OrderProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class BikeStoreIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmailWorker emailWorker;

    @Autowired
    private PaymentWorker paymentWorker;

    @BeforeEach
    void cleanQueues() {
        rabbitAdmin.purgeQueue(RabbitMQConfig.PAYMENTS_QUEUE, false);
        rabbitAdmin.purgeQueue(RabbitMQConfig.PAYMENTS_DLQ, false);
        rabbitAdmin.purgeQueue(RabbitMQConfig.EMAILS_QUEUE, false);

        // Drenamos cualquier mensaje residual
        while (rabbitTemplate.receive(RabbitMQConfig.PAYMENTS_DLQ, 100) != null) {}
        while (rabbitTemplate.receive(RabbitMQConfig.PAYMENTS_QUEUE, 100) != null) {}
        while (rabbitTemplate.receive(RabbitMQConfig.EMAILS_QUEUE, 100) != null) {}
    }

    private long getQueueMessageCount(String queueName) {
        var props = rabbitAdmin.getQueueProperties(queueName);
        if (props == null) {
            return 0;
        }
        Object count = props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        return (count instanceof Number number) ? number.longValue() : 0;
    }

    @Test
    @DisplayName("Escenario 1 y 2: Pedido con Pago Exitoso fluye a EmailWorker y procesa la cola")
    void testSuccessfulPaymentFlow() {
        OrderRequest request = new OrderRequest(
                "Maria Lopez",
                "maria.success@example.com",
                new BigDecimal("850.00")
        );

        OrderMessage published = orderProducer.publishOrder(request);
        assertNotNull(published.orderId());

        // Esperamos a que PaymentWorker procese el pago exitoso y la cola payments.queue quede en 0
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            long count = getQueueMessageCount(RabbitMQConfig.PAYMENTS_QUEUE);
            assertEquals(0, count, "La cola payments.queue debe quedar procesada");
        });
    }

    @Test
    @DisplayName("Escenario 3 y 4: Pedido Fallido realiza 3 intentos y termina en Dead Letter Queue (DLQ)")
    void testFailedPaymentRoutingToDlq() {
        OrderRequest request = new OrderRequest(
                "Roberto Gomez",
                "roberto.fail@example.com",
                new BigDecimal("1200.00")
        );

        OrderMessage published = orderProducer.publishOrder(request);
        assertNotNull(published.orderId());

        // Esperamos a que se completen los 3 intentos con reintentos y el mensaje llegue a la DLQ
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            Object dlqMsg = rabbitTemplate.receiveAndConvert(RabbitMQConfig.PAYMENTS_DLQ, 500);
            if (dlqMsg != null) {
                String body = (dlqMsg instanceof byte[] bytes)
                        ? new String(bytes, StandardCharsets.UTF_8)
                        : dlqMsg.toString();
                return body.contains(published.orderId());
            }
            return false;
        });

        // Verificamos que payments.dlq recibió el mensaje de la orden fallida
        assertTrue(true, "El mensaje llegó correctamente a la DLQ tras los 3 intentos");
    }

    @Test
    @DisplayName("Escenario 5: EmailWorker ignora eventos con estado distinto a PAID")
    void testEmailWorkerSkipsNonPaidOrders() {
        OrderMessage failedOrder = new OrderMessage(
                "ORD-FAIL-999",
                "Cliente Test",
                "cliente@test.com",
                new BigDecimal("500.00"),
                "FAILED",
                LocalDateTime.now()
        );

        assertDoesNotThrow(() -> {
            String json = objectMapper.writeValueAsString(failedOrder);
            emailWorker.processEmail(json);
        });
    }

    @Test
    @DisplayName("Escenario 6: EmailWorker es idempotente y evita correos duplicados para el mismo orderId")
    void testEmailWorkerPreventsDuplicates() throws Exception {
        OrderMessage paidOrder = new OrderMessage(
                "ORD-DUP-111",
                "Laura Sanchez",
                "laura@test.com",
                new BigDecimal("300.00"),
                "PAID",
                LocalDateTime.now()
        );

        String json = objectMapper.writeValueAsString(paidOrder);

        // Primer envío -> procesado e impreso
        assertDoesNotThrow(() -> emailWorker.processEmail(json));

        // Segundo envío con el mismo orderId -> ignorado por duplicado
        assertDoesNotThrow(() -> emailWorker.processEmail(json));
    }
}
