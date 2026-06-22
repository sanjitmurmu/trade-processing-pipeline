package com.sanjit.common.dto;

import java.math.BigDecimal;

public record ProcessedTradeEvent(
        String tradeId,
        Integer quantity,
        BigDecimal price,
        String exchange,
        String symbol,
        String currency,
        BigDecimal brokerageFee,
        BigDecimal tax
) {

}