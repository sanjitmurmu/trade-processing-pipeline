
package com.sanjit.common.dto;

import com.sanjit.common.enums.TradeSide;
import com.sanjit.common.enums.TradeStatus;

import java.math.BigDecimal;

public record ProcessedTradeEvent(
        String tradeId,
        String symbol,
        TradeSide side,
        Integer quantity,
        BigDecimal price,
        String exchange,
        String currency,
        String sector,
        BigDecimal brokerageFee,
        BigDecimal tax,
        BigDecimal regulatoryFee,
        BigDecimal totalFees,
        TradeStatus status
) {
}
