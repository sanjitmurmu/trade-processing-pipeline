package com.sanjit.referencedataservice.controller;

import com.sanjit.common.dto.ReferenceDataResponse;
import com.sanjit.referencedataservice.service.ReferenceDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/reference")
@RequiredArgsConstructor
public class ReferenceDataController {

    private final ReferenceDataService referenceDataService;

    @GetMapping("/{symbol}")
    public ResponseEntity<ReferenceDataResponse> getReferenceData(
            @PathVariable String symbol) {

        return referenceDataService.findBySymbol(symbol)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
