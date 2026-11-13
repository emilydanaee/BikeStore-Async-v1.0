package com.bikestore.async.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderMessage(

        String orderId,
        String customerName,
        String customerEmail,
        BigDecimal total,
        String paymentStatus,
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
        LocalDateTime timestamp

) {
    // Constructor de conveniencia para cuando el pedido se crea por primera vez
    public OrderMessage(String orderId, String customerName, String customerEmail, BigDecimal total, LocalDateTime timestamp) {
        this(orderId, customerName, customerEmail, total, "PENDING", timestamp);
    }
}
