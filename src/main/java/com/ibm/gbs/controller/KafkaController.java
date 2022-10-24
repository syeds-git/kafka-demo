package com.ibm.gbs.controller;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.service.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class KafkaController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @PostMapping(value = "/customer")
    public void sendCustomer(@RequestBody com.ibm.gbs.model.Customer model){
        Customer customer = Customer.newBuilder(new Customer(
                model.getCustomerId(),
                model.getName(),
                model.getPhoneNumber(),
                model.getAccountId()
        )).build();
        kafkaProducer.produceCustomer(customer);
    }

    @PostMapping(value = "/balance")
    public void sendBalance(@RequestBody com.ibm.gbs.model.Balance model){
        Balance balance = Balance.newBuilder(new Balance(
                UUID.randomUUID().toString(),
                model.getAccountId(),
                model.getBalance().floatValue()
        )).build();

        kafkaProducer.produceBalance(balance);
    }
}
