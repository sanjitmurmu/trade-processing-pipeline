package com.sanjit.tradeprocessor.service;

import com.sanjit.tradeprocessor.repository.TradeRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TradeService {

    private final TradeRepository tradeRepository;

}