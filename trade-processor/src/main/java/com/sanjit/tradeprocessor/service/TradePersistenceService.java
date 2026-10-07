package com.sanjit.tradeprocessor.service;

import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.entity.TradeEntity;
import com.sanjit.tradeprocessor.repository.OutboxEventRepository;
import com.sanjit.tradeprocessor.repository.TradeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TradePersistenceService {

    private final TradeRepository tradeRepository;
    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public void persistTradeAndEvent(
            TradeEntity tradeEntity,
            OutboxEvent outboxEvent) {

        tradeRepository.save(tradeEntity);
        outboxEventRepository.save(outboxEvent);
    }
}