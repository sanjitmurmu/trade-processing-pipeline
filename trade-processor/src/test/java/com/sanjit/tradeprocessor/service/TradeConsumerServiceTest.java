package com.sanjit.tradeprocessor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjit.common.dto.TradeEvent;
import com.sanjit.common.enums.TradeSide;
import com.sanjit.common.dto.ReferenceDataResponse;
import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.entity.TradeEntity;
import com.sanjit.tradeprocessor.enums.OutboxStatus;
import com.sanjit.tradeprocessor.repository.TradeRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TradeConsumerServiceTest {

    @Mock
    private TradeRepository tradeRepository;

    @Mock
    private ReferenceDataClient referenceDataClient;

    @Mock
    private FeeCalculationService feeCalculationService;

    @Mock
    private TradePersistenceService tradePersistenceService;


    private final ObjectMapper objectMapper = new ObjectMapper();

    private TradeConsumerService tradeConsumerService;


    @BeforeEach
    void setUp() {
        tradeConsumerService = new TradeConsumerService(
                tradeRepository,
                referenceDataClient,
                feeCalculationService,
                tradePersistenceService,
                objectMapper
        );
    }


    @Test
    void shouldPersistTradeAndOutboxWithProcessedTradePayload()
            throws Exception {

        TradeEvent tradeEvent = new TradeEvent(
                "TEST-FEE-001",
                "AAPL",
                TradeSide.BUY,
                10,
                new BigDecimal("100.00")
        );

        var referenceData = new ReferenceDataResponse(
                "AAPL",
                "NASDAQ",
                "USD",
                "TECH"
        );

        var fees = new FeeCalculationService.FeeBreakdown(
                new BigDecimal("1.0000"),
                new BigDecimal("0.5000"),
                new BigDecimal("0.1000"),
                new BigDecimal("1.6000")
        );

        when(tradeRepository.findByTradeId("TEST-FEE-001"))
                .thenReturn(java.util.Optional.empty());

        when(referenceDataClient.getReferenceData("AAPL"))
                .thenReturn(referenceData);

        when(feeCalculationService.calculateFees(
                new BigDecimal("100.00"), 10))
                .thenReturn(fees);

        tradeConsumerService.consumeTrade(tradeEvent);

        ArgumentCaptor<TradeEntity> tradeCaptor =
                ArgumentCaptor.forClass(TradeEntity.class);

        ArgumentCaptor<OutboxEvent> outboxCaptor =
                ArgumentCaptor.forClass(OutboxEvent.class);

        verify(tradePersistenceService).persistTradeAndEvent(
                tradeCaptor.capture(),
                outboxCaptor.capture()
        );

        TradeEntity savedTrade = tradeCaptor.getValue();
        OutboxEvent savedOutbox = outboxCaptor.getValue();

        assertEquals(new BigDecimal("1.0000"), savedTrade.getBrokerageFee());
        assertEquals(new BigDecimal("0.5000"), savedTrade.getTax());
        assertEquals(new BigDecimal("0.1000"), savedTrade.getRegulatoryFee());
        assertEquals(new BigDecimal("1.6000"), savedTrade.getTotalFees());

        assertEquals("trade-notifications", savedOutbox.getTopic());
        assertEquals(OutboxStatus.PENDING, savedOutbox.getStatus());

        var payloadJson = objectMapper.readTree(savedOutbox.getPayload());

        assertEquals("TEST-FEE-001",
                payloadJson.get("tradeId").asText());
        assertEquals("AAPL",
                payloadJson.get("symbol").asText());
        assertEquals("NASDAQ",
                payloadJson.get("exchange").asText());
        assertEquals("USD",
                payloadJson.get("currency").asText());
        assertEquals("TECH",
                payloadJson.get("sector").asText());


        assertEquals(0, new BigDecimal("1.0000")
                .compareTo(payloadJson.get("brokerageFee").decimalValue()));
        assertEquals(0, new BigDecimal("0.5000")
                .compareTo(payloadJson.get("tax").decimalValue()));
        assertEquals(0, new BigDecimal("0.1000")
                .compareTo(payloadJson.get("regulatoryFee").decimalValue()));
        assertEquals(0, new BigDecimal("1.6000")
                .compareTo(payloadJson.get("totalFees").decimalValue()));

        assertEquals("PENDING",
                payloadJson.get("status").asText());

        verify(feeCalculationService).calculateFees(
                new BigDecimal("100.00"), 10);


    }

}
