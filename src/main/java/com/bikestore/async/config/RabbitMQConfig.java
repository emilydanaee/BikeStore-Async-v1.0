package com.bikestore.async.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // Exchanges
    public static final String ORDERS_EXCHANGE = "orders.exchange";
    public static final String PAYMENTS_DLX = "payments.dlx";

    // Queues
    public static final String PAYMENTS_QUEUE = "payments.queue";
    public static final String PAYMENTS_DLQ = "payments.dlq";
    public static final String EMAILS_QUEUE = "emails.queue";

    // Routing Keys
    public static final String PAYMENT_CREATED_ROUTING_KEY = "payment.created";
    public static final String PAYMENT_FAILED_ROUTING_KEY = "payment.failed";
    public static final String PAYMENT_COMPLETED_ROUTING_KEY = "payment.completed";

    // Aliases para compatibilidad hacia atrás
    public static final String ORDER_EXCHANGE = ORDERS_EXCHANGE;
    public static final String PAYMENT_QUEUE = PAYMENTS_QUEUE;
    public static final String PAYMENT_ROUTING_KEY = PAYMENT_CREATED_ROUTING_KEY;
    public static final String PAYMENT_DLX = PAYMENTS_DLX;
    public static final String PAYMENT_DLQ = PAYMENTS_DLQ;
    public static final String PAYMENT_DLQ_KEY = PAYMENT_FAILED_ROUTING_KEY;
    public static final String EMAIL_QUEUE = EMAILS_QUEUE;
    public static final String PAYMENT_COMPLETED_KEY = PAYMENT_COMPLETED_ROUTING_KEY;

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(true);
        return admin;
    }

    @Bean
    public org.springframework.boot.ApplicationRunner initializeRabbitAdmin(RabbitAdmin rabbitAdmin) {
        return args -> rabbitAdmin.initialize();
    }

    // 1. Declaración de Exchanges Directos
    @Bean
    public DirectExchange ordersExchange() {
        return new DirectExchange(ORDERS_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange paymentsDlx() {
        return new DirectExchange(PAYMENTS_DLX, true, false);
    }

    // 2. Declaración de Colas Durables
    @Bean
    public Queue paymentsQueue() {
        return QueueBuilder.durable(PAYMENTS_QUEUE)
                .deadLetterExchange(PAYMENTS_DLX)
                .deadLetterRoutingKey(PAYMENT_FAILED_ROUTING_KEY)
                .build();
    }

    @Bean
    public Queue paymentsDlq() {
        return QueueBuilder.durable(PAYMENTS_DLQ).build();
    }

    @Bean
    public Queue emailsQueue() {
        return QueueBuilder.durable(EMAILS_QUEUE).build();
    }

    // 3. Declaración de Bindings
    @Bean
    public Binding paymentsBinding() {
        return BindingBuilder
                .bind(paymentsQueue())
                .to(ordersExchange())
                .with(PAYMENT_CREATED_ROUTING_KEY);
    }

    @Bean
    public Binding emailsBinding() {
        return BindingBuilder
                .bind(emailsQueue())
                .to(ordersExchange())
                .with(PAYMENT_COMPLETED_ROUTING_KEY);
    }

    @Bean
    public Binding paymentsDlqBinding() {
        return BindingBuilder
                .bind(paymentsDlq())
                .to(paymentsDlx())
                .with(PAYMENT_FAILED_ROUTING_KEY);
    }

    // 4. Estrategia de recuperación ante fallos agotados (DLQ)
    @Bean
    public MessageRecoverer messageRecoverer() {
        return new RejectAndDontRequeueRecoverer();
    }
}
