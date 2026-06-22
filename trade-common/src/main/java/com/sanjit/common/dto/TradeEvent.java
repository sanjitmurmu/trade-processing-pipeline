package com.sanjit.common.dto;

import com.sanjit.common.enums.TradeSide;

import java.math.BigDecimal;

public record TradeEvent(
        String tradeId,
        String symbol,
        TradeSide side,
        Integer quantity,
        BigDecimal price
) {
}