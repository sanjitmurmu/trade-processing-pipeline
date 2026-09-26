package com.sanjit.tradeprocessor.service;

import com.sanjit.common.constants.KafkaTopics;
import com.sanjit.common.dto.TradeEvent;
import com.sanjit.common.enums.TradeStatus;
import com.sanjit.tradeprocessor.entity.TradeEntity;
import com.sanjit.tradeprocessor.repository.TradeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class TradeConsumerService {

    private final TradeRepository tradeRepository;

    @KafkaListener(
            topics = KafkaTopics.TRADE_EVENTS,
            groupId = "trade-processor-group")
    public void consumeTrade(TradeEvent tradeEvent) {

        log.info("Received Trade : {}", tradeEvent.tradeId());

        TradeEntity entity = TradeEntity.builder()
                .tradeId(tradeEvent.tradeId())
                .symbol(tradeEvent.symbol())
                .side(tradeEvent.side())
                .quantity(tradeEvent.quantity())
                .price(tradeEvent.price())
                .exchange("NASDAQ")
                .currency("USD")
                .sector("TECH")
                .status(TradeStatus.PENDING)
                .build();

        tradeRepository.save(entity);

        log.info("Trade saved successfully");
    }
}