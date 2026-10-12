
package com.sanjit.tradeprocessor.service;

import com.sanjit.tradeprocessor.entity.FeeConfiguration;
import com.sanjit.tradeprocessor.enums.FeeType;
import com.sanjit.tradeprocessor.repository.FeeConfigurationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.mockito.Mockito.lenient;
import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FeeCalculationServiceTest {

    @Mock
    private FeeConfigurationRepository feeConfigurationRepository;

    @InjectMocks
    private FeeCalculationService feeCalculationService;

    @BeforeEach
    void setUp() {
        lenient().when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.BROKERAGE))
                .thenReturn(Optional.of(configuration(
                        FeeType.BROKERAGE, "0.10")));

        lenient().when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.TAX))
                .thenReturn(Optional.of(configuration(
                        FeeType.TAX, "0.18")));

        lenient().when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.REGULATORY))
                .thenReturn(Optional.of(configuration(
                        FeeType.REGULATORY, "0.005")));
    }

    @Test
    void shouldCalculateAllFeesIndependentlyFromTradeValue() {
        var result = feeCalculationService.calculateFees(
                new BigDecimal("10000.00"), 1);

        assertEquals(new BigDecimal("10.0000"), result.brokerageFee());
        assertEquals(new BigDecimal("18.0000"), result.tax());
        assertEquals(new BigDecimal("0.5000"), result.regulatoryFee());
        assertEquals(new BigDecimal("28.5000"), result.totalFees());

        verify(feeConfigurationRepository, times(1))
                .findByFeeTypeAndActiveTrue(FeeType.BROKERAGE);
        verify(feeConfigurationRepository, times(1))
                .findByFeeTypeAndActiveTrue(FeeType.TAX);
        verify(feeConfigurationRepository, times(1))
                .findByFeeTypeAndActiveTrue(FeeType.REGULATORY);
    }

    @Test
    void shouldRoundFeeToFourDecimalPlacesUsingHalfUp() {
        when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.BROKERAGE))
                .thenReturn(Optional.of(configuration(
                        FeeType.BROKERAGE, "0.0005")));

        var result = feeCalculationService.calculateFees(
                new BigDecimal("100"), 1);

        assertEquals(new BigDecimal("0.0005"), result.brokerageFee());
    }

    @Test
    void shouldRejectNonPositivePrice() {
        assertThrows(IllegalArgumentException.class,
                () -> feeCalculationService.calculateFees(
                        new BigDecimal("0"), 1));

        verifyNoInteractions(feeConfigurationRepository);
    }

    @Test
    void shouldRejectNonPositiveQuantity() {
        assertThrows(IllegalArgumentException.class,
                () -> feeCalculationService.calculateFees(
                        new BigDecimal("100"), 0));

        verifyNoInteractions(feeConfigurationRepository);
    }

    @Test
    void shouldFailWhenFeeConfigurationIsMissing() {
        when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.TAX))
                .thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class,
                () -> feeCalculationService.calculateFees(
                        new BigDecimal("10000"), 1));
    }

    @Test
    void shouldFailWhenFeeRateIsNegative() {
        when(feeConfigurationRepository.findByFeeTypeAndActiveTrue(
                FeeType.BROKERAGE))
                .thenReturn(Optional.of(configuration(
                        FeeType.BROKERAGE, "-0.10")));

        assertThrows(IllegalStateException.class,
                () -> feeCalculationService.calculateFees(
                        new BigDecimal("10000"), 1));
    }

    private FeeConfiguration configuration(
            FeeType feeType,
            String rate) {

        return FeeConfiguration.builder()
                .feeType(feeType)
                .rate(new BigDecimal(rate))
                .active(true)
                .build();
    }
}
