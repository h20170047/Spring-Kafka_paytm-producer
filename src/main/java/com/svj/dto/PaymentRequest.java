package com.svj.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentRequest {
    private String transactionId;
    private String sourceAccount;
    private String destnAccount;
    private double amount;
    private LocalDate txDate;
}
