package com.sanjit.tradeprocessor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.web.client.HttpClientErrorException;

@Configuration
public class KafkaConsumerConfig {

//    @Bean
//    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
//
//        ExponentialBackOffWithMaxRetries backOff =
//                new ExponentialBackOffWithMaxRetries(3);
//
//        backOff.setInitialInterval(1000L);
//        backOff.setMultiplier(2.0);
//        backOff.setMaxInterval(4000L);
//
//        DefaultErrorHandler errorHandler =
//                new DefaultErrorHandler(deadLetterPublishingRecoverer,backOff);
//
//        errorHandler.addNotRetryableExceptions(
//                HttpClientErrorException.NotFound.class
//        );
//
//        return errorHandler;
//    }
}