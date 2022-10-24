package com.ibm.gbs.service;

import com.ibm.gbs.schema.Balance;
import com.ibm.gbs.schema.Customer;
import com.ibm.gbs.schema.CustomerBalance;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class KafkaStreamProcessorTest {

    public static Map<String, Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, (String) value));
        return configs;
    }

    @Test
    public void shouldJoinCustomerAndBalance() {
        // setup
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        streamsProps.put("schema.registry.url", "mock://aggregation-test");

        final String customerTopic = "Customer";
        final String balanceTopic = "Balance";
        final String customerBalanceTopic = "CustomerBalance";
        final Map<String, Object> configMap = KafkaStreamProcessorTest.propertiesToMap(streamsProps);

        final Serde<Customer> customerSpecificSerde = new SpecificAvroSerde<>();
        customerSpecificSerde.configure(configMap, false);

        final Serde<Balance> balanceSpecificSerde = new SpecificAvroSerde<>();
        balanceSpecificSerde.configure(configMap, false);

        final Serde<CustomerBalance> custBalanceSpecificSerde = new SpecificAvroSerde<>();
        custBalanceSpecificSerde.configure(configMap, false);

        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        // read streams from mock input topics
        final KStream<String, Customer> customerStream = builder.stream(customerTopic, Consumed.with(Serdes.String(), customerSpecificSerde));
        final KStream<String, Balance> balanceStream = builder.stream(balanceTopic, Consumed.with(Serdes.String(), balanceSpecificSerde));

        // define topology
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
                JoinWindows.of(Duration.ofSeconds(30)),
                StreamJoined.with(Serdes.String(), customerSpecificSerde, balanceSpecificSerde)
        );

        customerBalance.to(customerBalanceTopic, Produced.with(Serdes.String(), custBalanceSpecificSerde));

        // execute topology
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            final TestInputTopic<String, Customer> customerTestTopic =
                    testDriver.createInputTopic(customerTopic,
                            stringSerde.serializer(),
                            customerSpecificSerde.serializer());

            final TestInputTopic<String, Balance> balanceTestTopic =
                    testDriver.createInputTopic(balanceTopic,
                            stringSerde.serializer(),
                            balanceSpecificSerde.serializer());

            final TestOutputTopic<String, CustomerBalance> outputTopic =
                    testDriver.createOutputTopic(customerBalanceTopic,
                            stringSerde.deserializer(),
                            custBalanceSpecificSerde.deserializer());

            // test inputs
            final List<Customer> customers = new ArrayList<>();
            customers.add(Customer.newBuilder().setCustomerId("test-customer-1").setName("test customer 1").setAccountId("42").setPhoneNumber("1234").build());
            customers.add(Customer.newBuilder().setCustomerId("test-customer-2").setName("test customer 2").setAccountId("24").setPhoneNumber("4321").build());
            customers.add(Customer.newBuilder().setCustomerId("test-customer-3").setName("test customer 3").setAccountId("33").setPhoneNumber("3333").build());

            final List<Balance> balances = new ArrayList<>();
            balances.add(Balance.newBuilder().setBalance(42.42f).setBalanceId("balance-1").setAccountId("24").build());
            balances.add(Balance.newBuilder().setBalance(12.12f).setBalanceId("balance-2").setAccountId("42").build());
            balances.add(Balance.newBuilder().setBalance(12.12f).setBalanceId("balance-3").setAccountId("66").build());

            // expected output
            final List<CustomerBalance> customerBalances = new ArrayList<>();
            customerBalances.add(CustomerBalance.newBuilder().setBalance(42.42f).setCustomerId("test-customer-2").setAccountId("24").setPhoneNumber("4321").build());
            customerBalances.add(CustomerBalance.newBuilder().setBalance(12.12f).setCustomerId("test-customer-1").setAccountId("42").setPhoneNumber("1234").build());

            customers.forEach(c -> customerTestTopic.pipeInput(c.getCustomerId().toString(), c));
            balances.forEach(b -> balanceTestTopic.pipeInput(b.getAccountId().toString(), b));
            List<CustomerBalance> actualValues = outputTopic.readValuesToList();
            // actualValues.forEach(System.out::println);

            // verify
            assertEquals(customerBalances, actualValues);
        }
    }
}
