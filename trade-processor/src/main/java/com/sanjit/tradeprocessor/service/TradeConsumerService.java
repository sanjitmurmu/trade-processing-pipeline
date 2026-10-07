package com.sanjit.tradeprocessor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sanjit.common.constants.KafkaTopics;
import com.sanjit.common.dto.ReferenceDataResponse;
import com.sanjit.common.dto.TradeEvent;
import com.sanjit.common.enums.TradeStatus;
import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.entity.OutboxStatus;
import com.sanjit.tradeprocessor.entity.TradeEntity;
import com.sanjit.tradeprocessor.repository.TradeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class TradeConsumerService {

    private final TradeRepository tradeRepository;
    private final ReferenceDataClient referenceDataClient;
    private final TradePersistenceService tradePersistenceService;
    private final ObjectMapper objectMapper;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(
                    delay = 1000,
                    multiplier = 2.0,
                    maxDelay = 4000
            )
    )
    @KafkaListener(
            topics = KafkaTopics.TRADE_EVENTS,
            groupId = "trade-processor-group"
    )
    public void consumeTrade(TradeEvent tradeEvent) {

        log.info("Received Trade : {}", tradeEvent.tradeId());

        if (tradeRepository.findByTradeId(tradeEvent.tradeId()).isPresent()) {
            log.info(
                    "Duplicate trade received. Skipping tradeId={}",
                    tradeEvent.tradeId()
            );
            return;
        }

        ReferenceDataResponse referenceData = referenceDataClient.getReferenceData(tradeEvent.symbol());

        TradeEntity entity = TradeEntity.builder()
                .tradeId(tradeEvent.tradeId())
                .symbol(tradeEvent.symbol())
                .side(tradeEvent.side())
                .quantity(tradeEvent.quantity())
                .price(tradeEvent.price())
                .exchange(referenceData.exchange())
                .currency(referenceData.currency())
                .sector(referenceData.sector())
                .status(TradeStatus.PENDING)
                .build();

        String payload;

        try {
            payload = objectMapper.writeValueAsString(tradeEvent);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to serialize trade event: " + tradeEvent.tradeId(),
                    e
            );
        }

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .eventType("TRADE_CREATED")
                .aggregateId(tradeEvent.tradeId())
                .topic("trade-notifications")
                .payload(payload)
                .status(OutboxStatus.PENDING)
                .build();

        tradePersistenceService.persistTradeAndEvent(entity, outboxEvent);

        log.info("Trade and outbox event saved successfully");
    }

    @DltHandler
    public void handleDlt(ConsumerRecord<String, TradeEvent> record) {

        TradeEvent tradeEvent = record.value();

        log.error(
                "Trade moved to DLT. tradeId={}, symbol={}, topic={}, partition={}, offset={}",
                tradeEvent.tradeId(),
                tradeEvent.symbol(),
                record.topic(),
                record.partition(),
                record.offset()
        );
    }
}