
package com.sanjit.tradeprocessor.service;

import com.sanjit.tradeprocessor.entity.FeeConfiguration;
import com.sanjit.tradeprocessor.enums.FeeType;
import com.sanjit.tradeprocessor.repository.FeeConfigurationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class FeeCalculationService {

    private static final BigDecimal ONE_HUNDRED =
            new BigDecimal("100");

    private static final int MONEY_SCALE = 4;

    private static final RoundingMode ROUNDING_MODE =
            RoundingMode.HALF_UP;

    private final FeeConfigurationRepository feeConfigurationRepository;

    public FeeBreakdown calculateFees(
            BigDecimal price,
            Integer quantity) {

        validateTradeInputs(price, quantity);

        BigDecimal tradeValue = price.multiply(
                BigDecimal.valueOf(quantity)
        );

        BigDecimal brokerageFee = calculateFee(
                tradeValue,
                getActiveRate(FeeType.BROKERAGE)
        );

        BigDecimal tax = calculateFee(
                tradeValue,
                getActiveRate(FeeType.TAX)
        );

        BigDecimal regulatoryFee = calculateFee(
                tradeValue,
                getActiveRate(FeeType.REGULATORY)
        );

        BigDecimal totalFees = brokerageFee
                .add(tax)
                .add(regulatoryFee);

        return new FeeBreakdown(
                brokerageFee,
                tax,
                regulatoryFee,
                totalFees
        );
    }

    private BigDecimal getActiveRate(FeeType feeType) {
        FeeConfiguration configuration =
                feeConfigurationRepository
                        .findByFeeTypeAndActiveTrue(feeType)
                        .orElseThrow(() ->
                                new IllegalStateException(
                                        "No active fee configuration found for "
                                                + feeType
                                )
                        );

        BigDecimal rate = configuration.getRate();

        if (rate == null || rate.signum() < 0) {
            throw new IllegalStateException(
                    "Invalid fee rate configured for " + feeType
            );
        }

        return rate;
    }

    private BigDecimal calculateFee(
            BigDecimal tradeValue,
            BigDecimal rate) {

        return tradeValue
                .multiply(rate)
                .divide(ONE_HUNDRED, MONEY_SCALE, ROUNDING_MODE);
    }

    private void validateTradeInputs(
            BigDecimal price,
            Integer quantity) {

        if (price == null || price.signum() <= 0) {
            throw new IllegalArgumentException(
                    "Trade price must be greater than zero"
            );
        }

        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException(
                    "Trade quantity must be greater than zero"
            );
        }
    }

    public record FeeBreakdown(
            BigDecimal brokerageFee,
            BigDecimal tax,
            BigDecimal regulatoryFee,
            BigDecimal totalFees
    ) {
    }
}
