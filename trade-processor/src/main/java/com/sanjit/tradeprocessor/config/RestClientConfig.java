package com.sanjit.tradeprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class RestClientConfig {

    @Value("${reference-data-service.url}")
    private String referenceDataServiceUrl;

    @Bean
    public RestClient restClient(){

        return RestClient.builder()
                .baseUrl(referenceDataServiceUrl)
                .build();
    }
}
