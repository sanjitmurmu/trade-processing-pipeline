package com.sanjit.tradeprocessor.service;

import com.sanjit.common.dto.ReferenceDataResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
@RequiredArgsConstructor
public class ReferenceDataClient {

    private final RestClient restClient;

    @Cacheable("referenceData")
    public ReferenceDataResponse getReferenceData(String symbol) {

        return restClient.get()
                .uri("/reference/{symbol}", symbol)
                .retrieve()
                .body(ReferenceDataResponse.class);
    }
}
