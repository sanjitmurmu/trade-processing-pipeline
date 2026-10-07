package com.sanjit.tradeprocessor.repository;

import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    List<OutboxEvent> findByStatus(OutboxStatus status);

}