
package com.sanjit.notificationservice;

import com.sanjit.common.dto.ProcessedTradeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ProcessedTradeEventListener {

    private static final Logger log =
            LoggerFactory.getLogger(ProcessedTradeEventListener.class);

    @KafkaListener(topics = "trade-notifications")
    public void handleProcessedTrade(ProcessedTradeEvent event) {
        log.info(
                "Processed trade notification received: tradeId={}, symbol={}, status={}, totalFees={}",
                event.tradeId(),
                event.symbol(),
                event.status(),
                event.totalFees()
        );
    }
}
