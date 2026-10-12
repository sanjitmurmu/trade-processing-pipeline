package com.sanjit.tradeprocessor.entity;

import com.sanjit.tradeprocessor.enums.FeeType;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;

@Entity
@Table(
        name = "fee_configuration",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_fee_configuration_type",
                        columnNames = "fee_type"
                )
        }
)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FeeConfiguration {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "fee_type", nullable = false, unique = true)
    private FeeType feeType;

    @Column(name = "rate", nullable = false, precision = 10, scale = 6)
    private BigDecimal rate;

    @Column(name = "active", nullable = false)
    private boolean active;
}

