package com.perkss.kafka.reactive

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import java.util.*


internal class OrderProcessingTopologyTest {

    @Test
    fun orderProcessing() {
        val builder = StreamsBuilder()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        props[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
        // TODO something is using these ouch
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java

        val appProperties = AppProperties()
        appProperties.applicationId = "test"
        appProperties.bootstrapServers = "dummy:1234"
        appProperties.customerInformation = "customer"
        appProperties.stockInventory = "stock"
        appProperties.orderRequest = "order-request"
        appProperties.outputTopic = "order-processed"

        val customer = customer(builder, appProperties)

        val stock = stock(builder, appProperties)

        val topology = orderProcessingTopology(props, builder, appProperties, customer, stock)

        val testDriver = TopologyTestDriver(topology, props)

        val stockTopic = testDriver.createInputTopic(appProperties.stockInventory, Serdes.String().serializer(), Serdes.String().serializer())
        stockTopic.pipeInput("key", "stock-matched")

        val customerTopic = testDriver.createInputTopic(appProperties.customerInformation, Serdes.String().serializer(), Serdes.String().serializer())
        customerTopic.pipeInput("key", "customer-matched")

        val inputTopic = testDriver.createInputTopic(appProperties.orderRequest, Serdes.String().serializer(), Serdes.String().serializer())
        inputTopic.pipeInput("key", "value")

        val outputTopic: TestOutputTopic<String, String> = testDriver.createOutputTopic(appProperties.outputTopic, Serdes.String().deserializer(), Serdes.String().deserializer())
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue("key", "customer-matched")))

    }
}