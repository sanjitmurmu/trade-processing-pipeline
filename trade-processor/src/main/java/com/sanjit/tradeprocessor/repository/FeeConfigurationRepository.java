
package com.sanjit.tradeprocessor.repository;

import com.sanjit.tradeprocessor.entity.FeeConfiguration;
import com.sanjit.tradeprocessor.enums.FeeType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface FeeConfigurationRepository
        extends JpaRepository<FeeConfiguration, Long> {

    Optional<FeeConfiguration> findByFeeTypeAndActiveTrue(FeeType feeType);
}
