package com.sanjit.tradeprocessor.repository;

import com.sanjit.tradeprocessor.entity.TradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface TradeRepository extends JpaRepository<TradeEntity, Long> {

    Optional<TradeEntity> findByTradeId(String tradeId);

}