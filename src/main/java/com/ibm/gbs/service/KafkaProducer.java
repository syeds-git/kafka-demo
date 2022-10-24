package com.ibm.gbs.service;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@Slf4j
public class KafkaProducer {

    @Value("${kafka.topic.customer}")
    String customerTopic;

    @Value("${kafka.topic.balance}")
    String balanceTopic;

    @Autowired
    private KafkaTemplate<String, Customer> kafkaCustomerTemplate;

    @Autowired
    private KafkaTemplate<String, Balance> kafkaBalanceTemplate;

    public void produceCustomer(Customer customer) {
        ListenableFuture<SendResult<String, Customer>> future=  kafkaCustomerTemplate.send(customerTopic, String.valueOf(customer.getCustomerId()), customer);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Customer>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error producing message", ex);
            }

            @Override
            public void onSuccess(SendResult<String, Customer> result) {
                log.info("Avro message successfully produced with key: {}", result.getProducerRecord().key());
            }
        });
    }

    public void produceBalance(Balance balance) {
        ListenableFuture<SendResult<String, Balance>> future=  kafkaBalanceTemplate.send(balanceTopic, String.valueOf(balance.getAccountId()), balance);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Balance>>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Error producing message", ex);
            }

            @Override
            public void onSuccess(SendResult<String, Balance> result) {
                log.info("Avro message successfully produced with key: {}", result.getProducerRecord().key());
            }
        });
    }
}
