package com.perkss.kafka.reactive.integration

import com.perkss.kafka.reactive.AppProperties
import com.perkss.kafka.reactive.model.Customer
import com.perkss.kafka.reactive.model.SchemaLoader
import com.perkss.kafka.reactive.model.toGenericRecord
import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRequested
import com.perkss.order.model.Stock
import io.confluent.common.utils.TestUtils
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.KafkaAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.KafkaStreams
import org.awaitility.Awaitility.await
import org.junit.Assert.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.support.TestPropertySourceUtils
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.*

@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = [StreamIntegrationTest.PropertyInit::class])
internal class StreamIntegrationTest @Autowired constructor(
    private var appProperties: AppProperties,
    private var orderProcessingApp: KafkaStreams
) {

    companion object {
        private val logger = LoggerFactory.getLogger(StreamIntegrationTest::class.java)
        lateinit var kafka: KafkaContainer
        lateinit var schemaRegistry: GenericContainer<Nothing>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            val kafkaImageName = DockerImageName.parse("confluentinc/cp-kafka:7.0.0")
            val schemaRegistryImageName = DockerImageName.parse("confluentinc/cp-schema-registry:7.0.0")
            kafka = KafkaContainer(kafkaImageName)
            kafka.apply {
                withNetwork(Network.newNetwork())
            }
            kafka.start()
            await().until { kafka.isRunning }
            schemaRegistry = GenericContainer(schemaRegistryImageName)

            schemaRegistry.apply {
                dependsOn(kafka)
                withNetwork(kafka.network)
                withExposedPorts(8081)
                withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://${kafka.networkAliases[0]}:9092")
                waitingFor(Wait.forLogMessage(".*Server started.*", 1))
                start()
            }

            val props = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            }

            // TODO can this reference the app properties and do it later?
            //  currently has to be done before spring app starts
            val kafkaAdminClient: AdminClient = KafkaAdminClient.create(props)
            val result: CreateTopicsResult = kafkaAdminClient
                .createTopics(
                    listOf(
                        "order-request", "order-processed",
                        "stock", "customer", "order-rejected"
                    )
                        .map { name -> NewTopic(name, 12, 1.toShort()) }
                        .toList())
            logger.info("Topics created {}", result.all().get())

        }
    }

    @Test
    fun `Sends a order request that is joined with stock and customer and order confirmed is responded`() {
        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer::class.java
            )
            put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer::class.java
            )
            put(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}"
            )
        }

        val consumerProps = Properties().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer::class.java
            )
            put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer::class.java
            )
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            put(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}"
            )
        }


        val testProducer = KafkaProducer<String, Any>(producerProps)
        val testConsumer = KafkaConsumer<String, OrderConfirmed>(consumerProps)

        val orderId = "e125125125gasfaseegakjgsaka"
        val productId = "e124121ffgaavasraseveas"
        val customerId = "f124f25125212156"
        val value = OrderRequested(orderId, productId, customerId)

        // Populate the stock table and the customer table
        testProducer.send(
            ProducerRecord(
                appProperties.customerInformation, customerId,
                Customer(customerId, "perkss", "london").toGenericRecord(SchemaLoader.loadSchema())
            )
        )

        testProducer.send(
            ProducerRecord(appProperties.stockInventory, productId, Stock(productId, "Party Poppers", 5))
        )

        // send Order Request message to topology
        testProducer.send(
            ProducerRecord(appProperties.orderRequest, orderId, value)
        )

        testConsumer.subscribe(listOf(appProperties.orderProcessedTopic))

        val actual = mutableMapOf<String, Pair<Int, OrderConfirmed>>()
        val expectedPartition = 3
        val expected = mapOf(orderId to (expectedPartition to OrderConfirmed(orderId, productId, customerId, true)))

        val timeout = System.currentTimeMillis() + 60000L
        while (actual.isEmpty() && System.currentTimeMillis() < timeout) {
            val records: ConsumerRecords<String, OrderConfirmed> = testConsumer.poll(Duration.ofSeconds(5))
            assertEquals(1, records.count())
            records.forEach { record -> actual[record.key()] = record.partition() to record.value() }
        }

        assertEquals(expected[orderId], actual[orderId])
        assertEquals(expectedPartition, actual[orderId]?.first)
        orderProcessingApp.close()
    }

    class PropertyInit : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                applicationContext,
                "perkss.kafka.example.bootstrap-servers=${kafka.bootstrapServers}",
                "perkss.kafka.example.schema-registry=http://${schemaRegistry.containerIpAddress}:${
                    schemaRegistry.getMappedPort(
                        8081
                    )
                }",
                "perkss.kafka.example.state-dir=${TestUtils.tempDirectory().path}"
            )
        }
    }
}