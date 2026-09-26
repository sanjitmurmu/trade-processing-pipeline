package com.sanjit.tradeproducer.service;

import com.sanjit.common.constants.KafkaTopics;
import com.sanjit.common.dto.TradeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TradeProducerService {

    private final KafkaTemplate<String, TradeEvent> kafkaTemplate;

    public void publishTrade(TradeEvent tradeEvent) {

        kafkaTemplate.send(
                KafkaTopics.TRADE_EVENTS,
                tradeEvent.tradeId(),
                tradeEvent);

        System.out.println(
                "Trade sent : " + tradeEvent.tradeId());
    }
}
