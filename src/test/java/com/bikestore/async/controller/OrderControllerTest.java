package com.bikestore.async.controller;

import com.bikestore.async.model.OrderMessage;
import com.bikestore.async.model.OrderRequest;
import com.bikestore.async.producer.OrderProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private OrderProducer orderProducer;

    @Test
    @DisplayName("Escenario 1: Pedido recibido - POST /api/orders debe responder HTTP 202 y datos del pedido")
    void testCreateOrderAccepted() throws Exception {
        OrderMessage mockMessage = new OrderMessage(
                "ORD-TEST-1234",
                "Carlos Perez",
                "carlos.perez@example.com",
                new BigDecimal("1500.00"),
                LocalDateTime.of(2026, 11, 12, 17, 30, 0)
        );

        when(orderProducer.publishOrder(any(OrderRequest.class))).thenReturn(mockMessage);

        String jsonPayload = """
                {
                    "customerName": "Carlos Perez",
                    "customerEmail": "carlos.perez@example.com",
                    "total": 1500.00
                }
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonPayload))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.orderId").value("ORD-TEST-1234"))
                .andExpect(jsonPath("$.customerName").value("Carlos Perez"))
                .andExpect(jsonPath("$.customerEmail").value("carlos.perez@example.com"))
                .andExpect(jsonPath("$.total").value(1500.00));
    }

    @Test
    @DisplayName("Debe responder HTTP 400 Bad Request si el pedido tiene datos inválidos")
    void testCreateOrderValidationFailure() throws Exception {
        String invalidPayload = """
                {
                    "customerName": "",
                    "customerEmail": "correo-invalido",
                    "total": -10.00
                }
                """;

        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidPayload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.details.customerName").exists())
                .andExpect(jsonPath("$.details.customerEmail").exists())
                .andExpect(jsonPath("$.details.total").exists());
    }
}
