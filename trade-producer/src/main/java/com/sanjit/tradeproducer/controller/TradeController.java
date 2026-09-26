package com.sanjit.tradeproducer.controller;

import com.sanjit.tradeproducer.service.TradeProducerService;
import com.sanjit.common.dto.TradeEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/trades")
@RequiredArgsConstructor
public class TradeController {

    private final TradeProducerService tradeProducerService;

    @PostMapping
    public ResponseEntity<String> createTrade(
            @RequestBody TradeEvent tradeEvent) {

        tradeProducerService.publishTrade(tradeEvent);

        return ResponseEntity.ok("Trade published successfully");
    }
}