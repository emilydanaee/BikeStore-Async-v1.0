package com.bikestore.async.producer;

import com.bikestore.async.config.RabbitMQConfig;
import com.bikestore.async.model.OrderMessage;
import com.bikestore.async.model.OrderRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    public OrderProducer(
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    public OrderMessage publishOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        OrderMessage order = new OrderMessage(
                orderId,
                request.customerName(),
                request.customerEmail(),
                request.total(),
                now
        );

        try {
            String json = objectMapper.writeValueAsString(order);

            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.ORDERS_EXCHANGE,
                    RabbitMQConfig.PAYMENT_CREATED_ROUTING_KEY,
                    json
            );

            logEvent(
                    orderId,
                    "ORDER_PUBLISHED",
                    "Pedido encolado exitosamente hacia RabbitMQ (Exchange: "
                            + RabbitMQConfig.ORDERS_EXCHANGE
                            + ", RoutingKey: " + RabbitMQConfig.PAYMENT_CREATED_ROUTING_KEY + ")"
            );

            return order;
        } catch (JacksonException e) {
            logEvent(orderId, "ORDER_SERIALIZATION_FAILED", "Fallo al serializar pedido a JSON: " + e.getMessage());
            throw new RuntimeException("Error al serializar el pedido utilizando Jackson 3", e);
        }
    }

    private void logEvent(
            String orderId,
            String event,
            String description
    ) {
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
