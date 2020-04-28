package com.perkss.kafka.reactive

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.PrimitiveAvroSerde
import org.apache.kafka.streams.*
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import java.util.*


internal class OrderProcessingTopologyTest {

    private val schemaRegistryScope: String = OrderProcessingTopologyTest::class.java.getName()
    private val mockSchemaRegistryUrl = "mock://$schemaRegistryScope"

    @Test
    fun orderProcessing() {
        val builder = StreamsBuilder()

        val props = Properties()
        val appId = "testOrderProcessing"
        val broker = "dummy:1234"

        props[StreamsConfig.APPLICATION_ID_CONFIG] = appId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        props[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
        // TODO something is using these ouch
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = PrimitiveAvroSerde::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = PrimitiveAvroSerde::class.java
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = mockSchemaRegistryUrl

        val config = mapOf(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to mockSchemaRegistryUrl)

        val keySerde = PrimitiveAvroSerde<String>()
        val valSerde = PrimitiveAvroSerde<String>()

        // configure with schema registry
        keySerde.configure(config, true)
        valSerde.configure(config, false)

        val appProperties = AppProperties()
        appProperties.applicationId = appId
        appProperties.bootstrapServers = broker
        appProperties.customerInformation = "customer"
        appProperties.stockInventory = "stock"
        appProperties.orderRequest = "order-request"
        appProperties.outputTopic = "order-processed"

        val customer = customer(builder, appProperties)

        val stock = stock(builder, appProperties)

        val topology = orderProcessingTopology(props, builder, appProperties, customer, stock, keySerde, valSerde)

        val testDriver = TopologyTestDriver(topology, props)

        val stockTopic = testDriver.createInputTopic(appProperties.stockInventory, keySerde.serializer(), valSerde.serializer())
        stockTopic.pipeInput("key", "stock-matched")

        val customerTopic = testDriver.createInputTopic(appProperties.customerInformation, keySerde.serializer(), valSerde.serializer())
        customerTopic.pipeInput("key", "customer-matched")

        val inputTopic = testDriver.createInputTopic(appProperties.orderRequest, keySerde.serializer(), valSerde.serializer())
        inputTopic.pipeInput("key", "value")

        val outputTopic: TestOutputTopic<String, String> = testDriver.createOutputTopic(appProperties.outputTopic, keySerde.deserializer(), valSerde.deserializer())
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("key", "customer-matched")))

    }
}