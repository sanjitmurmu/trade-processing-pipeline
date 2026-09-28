package com.sanjit.referencedataservice.service;

import com.sanjit.common.dto.ReferenceDataResponse;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Service
public class ReferenceDataService {

    private static final Map<String, ReferenceDataResponse> SECURITIES = Map.of(
            "AAPL", new ReferenceDataResponse("AAPL", "NASDAQ", "USD", "TECH"),
            "MSFT", new ReferenceDataResponse("MSFT", "NASDAQ", "USD", "TECH"),
            "NVDA", new ReferenceDataResponse("NVDA", "NASDAQ", "USD", "TECH"),
            "JPM", new ReferenceDataResponse("JPM", "NYSE", "USD", "FINANCIALS"),
            "JNJ", new ReferenceDataResponse("JNJ", "NYSE", "USD", "HEALTHCARE")
    );

    public Optional<ReferenceDataResponse> findBySymbol(String symbol) {
        return Optional.ofNullable(SECURITIES.get(symbol));
    }
}