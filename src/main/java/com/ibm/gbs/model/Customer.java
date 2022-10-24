package com.ibm.gbs.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Customer {
    String customerId;
    String name;
    String phoneNumber;
    String accountId;
}
