package com.bikestore.async.model;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record OrderRequest(

        @NotBlank(message = "El nombre del cliente no puede estar vacío")
        String customerName,

        @NotBlank(message = "El correo electrónico es obligatorio")
        @Email(message = "El correo electrónico debe tener un formato válido")
        String customerEmail,

        @NotNull(message = "El monto total es obligatorio")
        @Positive(message = "El monto total debe ser mayor que cero")
        BigDecimal total

) {}
