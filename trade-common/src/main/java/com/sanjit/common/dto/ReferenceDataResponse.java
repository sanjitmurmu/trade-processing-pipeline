package com.sanjit.common.dto;

public record ReferenceDataResponse(

        String symbol,

        String exchange,

        String currency,

        String sector

) {

}