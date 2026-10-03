package com.sanjit.common.dto;

import com.sanjit.common.enums.TradeSide;

import java.math.BigDecimal;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record TradeEvent(

        @NotBlank
        String tradeId,

        @NotBlank
        String symbol,

        @NotNull
        TradeSide side,

        @NotNull
        @Positive
        Integer quantity,

        @NotNull
        @Positive
        BigDecimal price
) {
}