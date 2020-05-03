package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.OrderProcessingTopology.customer
import com.perkss.kafka.reactive.OrderProcessingTopology.orderProcessing
import com.perkss.kafka.reactive.OrderProcessingTopology.stock
import com.perkss.kafka.reactive.model.Customer
import com.perkss.kafka.reactive.model.SchemaLoader
import com.perkss.kafka.reactive.model.toGenericRecord
import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRequested
import com.perkss.order.model.Stock
import io.confluent.common.utils.TestUtils
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import java.util.*

class OrderProcessingTopologyTest {

    private val schemaRegistryScope: String = OrderProcessingTopologyTest::class.java.name
    private val mockSchemaRegistryUrl = "mock://$schemaRegistryScope"

    @Test
    fun orderProcessingCustomerOrders() {
        val builder = StreamsBuilder()

        val props = Properties()
        val appId = "testOrderProcessing"
        val broker = "dummy:1234"

        props[StreamsConfig.APPLICATION_ID_CONFIG] = appId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        props[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = mockSchemaRegistryUrl
        props[StreamsConfig.STATE_DIR_CONFIG] = TestUtils.tempDirectory().path

        val appProperties = AppProperties()
        appProperties.applicationId = appId
        appProperties.bootstrapServers = broker
        appProperties.customerInformation = "customer"
        appProperties.stockInventory = "stock"
        appProperties.orderRequest = "order-request"
        appProperties.orderProcessedTopic = "order-processed"
        appProperties.schemaRegistry = mockSchemaRegistryUrl

        val config = mapOf(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to mockSchemaRegistryUrl)

        val schemaRegistryClient = MockSchemaRegistry.getClientForScope(schemaRegistryScope)

        schemaRegistryClient.register("${appProperties.customerInformation}-value", SchemaLoader.loadSchema())
        schemaRegistryClient.register("${appProperties.orderRequest}-value", SchemaLoader.loadSchema())

        val keySerde = Serdes.String()
        val valSerde = Serdes.String()
        val orderRequestSerde = SpecificAvroSerde<OrderRequested>(schemaRegistryClient)
        val orderProcessedSerde = SpecificAvroSerde<OrderConfirmed>(schemaRegistryClient)
        val stockSerde = SpecificAvroSerde<Stock>(schemaRegistryClient)
        val genericAvroSerde = GenericAvroSerde(schemaRegistryClient)

        // configure with schema registry
        keySerde.configure(config, true)
        valSerde.configure(config, false)
        orderRequestSerde.configure(config, false)
        genericAvroSerde.configure(config, false)
        stockSerde.configure(config, false)
        orderProcessedSerde.configure(config, false)

        val customer = customer(builder, appProperties, keySerde, genericAvroSerde)

        val stock = stock(builder, appProperties)

        val topology = orderProcessing(props, builder, appProperties, customer, stock, keySerde, orderRequestSerde, orderProcessedSerde)

        val testDriver = TopologyTestDriver(topology, props)
        val orderId = "1234A"
        val productId = "12412"
        val customerId = "AAAAA"
        val stockTopic = testDriver.createInputTopic(appProperties.stockInventory, keySerde.serializer(), stockSerde.serializer())
        stockTopic.pipeInput(productId, Stock("AA", productId, 1))

        // Customer is populated with GenericAvroSerde customer details
        val customerTopic: TestInputTopic<String, GenericRecord> =
                testDriver.createInputTopic(appProperties.customerInformation, Serdes.String().serializer(), genericAvroSerde.serializer())

        val customerRecord = Customer(customerId, "perkss", "london")
                .toGenericRecord(SchemaLoader.loadSchema())

        customerTopic.pipeKeyValueList(
                Collections.singletonList(
                        KeyValue.pair(customerRecord.get("id") as String, customerRecord)))

        val inputTopic = testDriver.createInputTopic(appProperties.orderRequest, keySerde.serializer(), orderRequestSerde.serializer())
        inputTopic.pipeInput(orderId, OrderRequested(orderId, productId, customerId))

        val outputTopic = testDriver.createOutputTopic(appProperties.orderProcessedTopic, keySerde.deserializer(), orderProcessedSerde.deserializer())
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(orderId, OrderConfirmed(orderId, productId, customerId, true))))

        testDriver.close()
        MockSchemaRegistry.dropScope(schemaRegistryScope)
    }
}