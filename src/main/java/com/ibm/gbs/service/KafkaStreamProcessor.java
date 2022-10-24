package com.ibm.gbs.service;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

@Component
@Slf4j
public class KafkaStreamProcessor {

    @Value("${window-duration-secs}")
    private Long windowDurationSecs;

    @Value("${kafka.topic.customer}")
    private String customerTopic;

    @Value("${kafka.topic.balance}")
    private String balanceTopic;

    @Value("${kafka.topic.customer-balance}")
    private String customerBalanceTopic;

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaUrl;

    @Autowired
    private StreamsBuilder streamsBuilder;

    @PostConstruct
    public void streamTopology() {
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", schemaUrl);

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        final Serde<Customer> customerSpecificSerde = new SpecificAvroSerde<>();
        customerSpecificSerde.configure(serdeConfig, false);

        final Serde<Balance> balanceSpecificSerde = new SpecificAvroSerde<>();
        balanceSpecificSerde.configure(serdeConfig, false);

        final Serde<CustomerBalance> custBalanceSpecificSerde = new SpecificAvroSerde<>();
        custBalanceSpecificSerde.configure(serdeConfig, false);

        KStream<String, Customer> customerStream = streamsBuilder.stream(customerTopic, Consumed.with(Serdes.String(), customerSpecificSerde));
        KStream<String, Balance> balanceStream = streamsBuilder.stream(balanceTopic, Consumed.with(Serdes.String(), balanceSpecificSerde));

        // change key to accountId
        KStream<String, Customer> customerAccountStream = customerStream.selectKey((k, v) -> v.getAccountId().toString());

        KStream<String, CustomerBalance> customerBalance = customerAccountStream.join(balanceStream, (customer, balance) -> {
                    CustomerBalance cb = CustomerBalance.newBuilder(new CustomerBalance(
                            customer.getAccountId(),
                            customer.getCustomerId(),
                            customer.getPhoneNumber(),
                            balance.getBalance()
                    )).build();
                    return cb;
                },
                JoinWindows.of(Duration.ofSeconds(windowDurationSecs)),
                StreamJoined.with(Serdes.String(), customerSpecificSerde, balanceSpecificSerde)
        );

        customerBalance.to(customerBalanceTopic, Produced.with(Serdes.String(), custBalanceSpecificSerde));

        customerBalance.peek((k, v) -> log.info("CustomerBalance for the key: {} is: {}", k, v));
    }
}
