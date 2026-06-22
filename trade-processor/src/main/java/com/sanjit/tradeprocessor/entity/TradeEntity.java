package com.sanjit.tradeprocessor.entity;

import com.sanjit.common.enums.TradeSide;
import com.sanjit.common.enums.TradeStatus;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "trade")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(
            name = "trade_id",
            nullable = false,
            unique = true
    )
    private String tradeId;

    @Column(
            name = "symbol",
            nullable = false
    )
    private String symbol;

    @Enumerated(EnumType.STRING)
    @Column(name = "side",nullable = false)
    private TradeSide side;

    @Column(name = "quantity",nullable = false)
    private Integer quantity;

    @Column(
            name = "price",
            nullable = false,
            precision = 19,
            scale = 4
    )
    private BigDecimal price;

    @Column(name = "exchange",nullable = false)
    private String exchange;

    @Column(name = "currency",nullable = false)
    private String currency;

    @Column(name = "sector",nullable = false)
    private String sector;

    @Column(
            name = "brokerage_fee",
            precision = 19,
            scale = 4
    )
    private BigDecimal brokerageFee;

    @Column(
            name = "tax",
            precision = 19,
            scale = 4
    )
    private BigDecimal tax;

    @Enumerated(EnumType.STRING)
    @Column(name = "status",nullable = false)
    private TradeStatus status;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    public void prePersist() {

        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();

    }

    @PreUpdate
    public void preUpdate() {

        updatedAt = LocalDateTime.now();

    }

}