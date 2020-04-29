package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.model.Customer
import com.perkss.kafka.reactive.model.SchemaLoader
import com.perkss.kafka.reactive.model.toGenericRecord
import com.perkss.order.model.OrderRequested
import io.confluent.common.utils.TestUtils
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import java.util.*

// good reference https://github.com/confluentinc/kafka-streams-examples/blob/5.4.1-post/src/test/java/io/confluent/examples/streams/GenericAvroIntegrationTest.java#L54
class OrderProcessingTopologyTest {

    private val schemaRegistryScope: String = OrderProcessingTopologyTest::class.java.name
    private val mockSchemaRegistryUrl = "mock://$schemaRegistryScope"

    // seerdes messed up with avro
    @Test
    fun orderProcessing() {
        val builder = StreamsBuilder()

        val props = Properties()
        val appId = "testOrderProcessing"
        val broker = "dummy:1234"

        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        props[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = mockSchemaRegistryUrl
        props[StreamsConfig.STATE_DIR_CONFIG] = TestUtils.tempDirectory().path

        val appProperties = AppProperties()
        appProperties.applicationId = "test"
        appProperties.bootstrapServers = "dummy:1234"
        appProperties.customerInformation = "customer"
        appProperties.stockInventory = "stock"
        appProperties.orderRequest = "order-request"
        appProperties.outputTopic = "order-processed"

        val config = mapOf(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to mockSchemaRegistryUrl)

        val schemaRegistryClient = MockSchemaRegistry.getClientForScope(schemaRegistryScope)

        // TODO register all topics with Avro
        schemaRegistryClient.register("customer-value", SchemaLoader.loadSchema())

        val keySerde = Serdes.String()
        val valSerde = Serdes.String()
        val orderRequestSerde = SpecificAvroSerde<OrderRequested>(schemaRegistryClient)
        val genericAvroSerde = GenericAvroSerde(schemaRegistryClient)

        // configure with schema registry
        keySerde.configure(config, true)
        valSerde.configure(config, false)
        orderRequestSerde.configure(config, false)
        genericAvroSerde.configure(config, false)

        val customer = customer(builder, appProperties)

        val stock = stock(builder, appProperties)

        val topology = orderProcessing(props, builder, appProperties, customer, stock, keySerde, orderRequestSerde)

        TopologyTestDriver(topology, props).use { testDriver ->
            val orderId = "1234A"
            val productId = "12412"


            val stockTopic = testDriver.createInputTopic(appProperties.stockInventory, keySerde.serializer(), valSerde.serializer())
            stockTopic.pipeInput(productId, "stock-matched")

            // Customer is populated with GenericAvroSerde customer details
            val avroSerializer = KafkaAvroSerializer(schemaRegistryClient)
            val customerTopic: TestInputTopic<String, Any> = testDriver.createInputTopic(appProperties.customerInformation, Serdes.String().serializer(), avroSerializer)
            val customerRecord = Customer("11111", "perkss", "london").toGenericRecord(SchemaLoader.loadSchema())
            customerTopic.pipeKeyValueList(
                    Collections.singletonList(
                            KeyValue.pair(customerRecord.get("id") as String, customerRecord as Any)))

            val inputTopic = testDriver.createInputTopic(appProperties.orderRequest, keySerde.serializer(), orderRequestSerde.serializer())
            inputTopic.pipeInput(orderId, OrderRequested(orderId, productId))

            val outputTopic = testDriver.createOutputTopic(appProperties.outputTopic, keySerde.deserializer(), valSerde.deserializer())
            assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(orderId, "customer-matched")))

            testDriver.close()
            MockSchemaRegistry.dropScope(schemaRegistryScope)
        }


    }
}