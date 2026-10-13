
package com.sanjit.tradeprocessor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjit.common.dto.ProcessedTradeEvent;
import com.sanjit.common.enums.TradeSide;
import com.sanjit.common.enums.TradeStatus;
import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.enums.OutboxStatus;
import com.sanjit.tradeprocessor.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherServiceTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private OutboxPublisherService outboxPublisherService;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();

        outboxPublisherService = new OutboxPublisherService(
                outboxEventRepository,
                kafkaTemplate,
                objectMapper
        );
    }

    @Test
    void shouldDeserializePayloadPublishEventAndMarkAsPublished()
            throws Exception {

        ProcessedTradeEvent processedTradeEvent =
                new ProcessedTradeEvent(
                        "T-101",
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

        String payload =
                objectMapper.writeValueAsString(processedTradeEvent);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId("T-101")
                .topic("trade-notifications")
                .payload(payload)
                .status(OutboxStatus.PENDING)
                .build();

        when(outboxEventRepository.findByStatus(OutboxStatus.PENDING))
                .thenReturn(List.of(outboxEvent));

        when(kafkaTemplate.send(
                eq("trade-notifications"),
                eq("T-101"),
                any(ProcessedTradeEvent.class)
        )).thenReturn(CompletableFuture.completedFuture(null));

        outboxPublisherService.publishPendingEvents();

        verify(kafkaTemplate).send(
                eq("trade-notifications"),
                eq("T-101"),
                any(ProcessedTradeEvent.class)
        );

        assertEquals(OutboxStatus.PUBLISHED, outboxEvent.getStatus());

        verify(outboxEventRepository).save(outboxEvent);
    }


    @Test
    void shouldKeepOutboxEventPendingWhenKafkaSendFails() throws Exception {

        ProcessedTradeEvent processedTradeEvent =
                new ProcessedTradeEvent(
                        "T-102",
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

        String payload =
                objectMapper.writeValueAsString(processedTradeEvent);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId("T-102")
                .topic("trade-notifications")
                .payload(payload)
                .status(OutboxStatus.PENDING)
                .build();

        when(outboxEventRepository.findByStatus(OutboxStatus.PENDING))
                .thenReturn(List.of(outboxEvent));

        CompletableFuture<SendResult<String, Object>> failedFuture =
                new CompletableFuture<>();

        failedFuture.completeExceptionally(
                new RuntimeException("Simulated Kafka send failure")
        );

        when(kafkaTemplate.send(
                eq("trade-notifications"),
                eq("T-102"),
                any(ProcessedTradeEvent.class)
        )).thenReturn(failedFuture);

        outboxPublisherService.publishPendingEvents();

        assertEquals(OutboxStatus.PENDING, outboxEvent.getStatus());

        verify(outboxEventRepository, never()).save(outboxEvent);
    }


    @Test
    void shouldKeepOutboxEventPendingWhenPayloadIsInvalid() {

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId("T-103")
                .topic("trade-notifications")
                .payload("{invalid-json")
                .status(OutboxStatus.PENDING)
                .build();

        when(outboxEventRepository.findByStatus(OutboxStatus.PENDING))
                .thenReturn(List.of(outboxEvent));

        outboxPublisherService.publishPendingEvents();

        assertEquals(OutboxStatus.PENDING, outboxEvent.getStatus());

        verify(kafkaTemplate, never()).send(
                eq("trade-notifications"),
                eq("T-103"),
                any(ProcessedTradeEvent.class)
        );

        verify(outboxEventRepository, never()).save(outboxEvent);
    }



}
