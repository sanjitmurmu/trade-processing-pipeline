
package com.sanjit.notificationservice;

import com.sanjit.common.dto.ProcessedTradeEvent;
import com.sanjit.common.enums.TradeSide;
import com.sanjit.common.enums.TradeStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class ProcessedTradeEventListenerTest {

    @InjectMocks
    private ProcessedTradeEventListener listener;

    @Test
    void shouldHandleProcessedTradeEventWithoutException() {

        ProcessedTradeEvent event = new ProcessedTradeEvent(
                "T-201",
                "AAPL",
                TradeSide.BUY,
                10,
                new BigDecimal("100.00"),
                "NASDAQ",
                "USD",
                "Technology",
                new BigDecimal("0.5000"),
                new BigDecimal("1.0000"),
                new BigDecimal("0.1000"),
                new BigDecimal("1.6000"),
                TradeStatus.PENDING
        );

        assertDoesNotThrow(() -> listener.handleProcessedTrade(event));
    }
}
