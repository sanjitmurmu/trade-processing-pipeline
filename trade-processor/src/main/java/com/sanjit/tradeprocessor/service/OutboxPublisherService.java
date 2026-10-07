package com.sanjit.tradeprocessor.service;

import com.sanjit.tradeprocessor.entity.OutboxEvent;
import com.sanjit.tradeprocessor.entity.OutboxStatus;
import com.sanjit.tradeprocessor.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisherService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(initialDelay = 60000, fixedDelay = 60000)
    public void publishPendingEvents() {

        log.info("Outbox publisher scheduler running at {}", LocalDateTime.now());

        List<OutboxEvent> pendingEvents =
                outboxEventRepository.findByStatus(OutboxStatus.PENDING);

        for (OutboxEvent event : pendingEvents) {

            try {
                kafkaTemplate
                        .send(
                                event.getTopic(),
                                event.getAggregateId(),
                                event.getPayload()
                        )
                        .get();

                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(LocalDateTime.now());

                outboxEventRepository.save(event);

            } catch (Exception e) {
                // Keep the event PENDING so the next scheduler run can retry it
            }
        }
    }
}