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
import org.junit.Assert
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
import org.testcontainers.containers.wait.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.*

@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = [StreamIntegrationTest.PropertyInit::class])
class StreamIntegrationTest {

    // TODO move into constructor
    @Autowired
    private lateinit var appProperties: AppProperties

    @Autowired
    private lateinit var orderProcessingApp: KafkaStreams

    companion object {
        private val logger = LoggerFactory.getLogger(StreamIntegrationTest::class.java)
        lateinit var kafka: KafkaContainer
        lateinit var schemaRegistry: GenericContainer<Nothing>

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            kafka = KafkaContainer("5.5.0")
            kafka.apply {
                withNetworkAliases("kafka-cluster")
            }
            kafka.start()
            await().until { kafka.isRunning }
            schemaRegistry = GenericContainer("confluentinc/cp-schema-registry:5.5.0")

            schemaRegistry.apply {
                dependsOn(kafka)
                withNetwork(kafka.network)
                withNetworkAliases("schema-registry")
                withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka-cluster:9092")
                waitingFor(Wait.forLogMessage(".*Server started.*", 1))
                start()
            }

            val producerProps = Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer::class.java)
                put("schema.registry.url", "http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}")
            }

            // TODO can this reference the app properties and do it later?
            //  currently has to be done before spring app starts
            val kafkaAdminClient: AdminClient = KafkaAdminClient.create(producerProps)
            val result: CreateTopicsResult = kafkaAdminClient
                    .createTopics(
                            listOf("order-request", "order-processed",
                                    "stock", "customer")
                                    .map { name -> NewTopic(name, 3, 1.toShort()) }
                                    .toList())
            logger.info("Topics created {}", result.all().get())

        }
    }

    @Test
    fun `Sends a order request that is joined with stock and customer and order confirmed is responded`() {

        val producerProps = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer::class.java)
            put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}")
        }

        val consumerProps = Properties().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer::class.java)
            put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}")
        }


        val testProducer = KafkaProducer<String, Any>(producerProps)
        val testConsumer = KafkaConsumer<String, OrderConfirmed>(consumerProps)

        val key = "1"
        val productId = "2"
        val customerId = "3"
        val value = OrderRequested(key, productId, customerId)

        // Populate the stock table and the customer table
        testProducer.send(
                ProducerRecord(appProperties.customerInformation, customerId,
                        Customer(customerId, "perkss", "london").toGenericRecord(SchemaLoader.loadSchema())))

        testProducer.send(
                ProducerRecord(appProperties.stockInventory, productId, Stock(productId, productId, 5)))

        // send Order Request message to topology
        testProducer.send(
                ProducerRecord(appProperties.orderRequest, key, value))

        testConsumer.subscribe(listOf(appProperties.orderProcessedTopic))

        val actual = mutableMapOf<String, OrderConfirmed>()
        val expected = mapOf(key to OrderConfirmed(key, productId, customerId, true))

        val timeout = System.currentTimeMillis() + 30000L;
        while (actual != expected && System.currentTimeMillis() < timeout) {
            val records: ConsumerRecords<String, OrderConfirmed> = testConsumer.poll(Duration.ofSeconds(1))
            records.forEach { record -> actual[record.key()] = record.value() }
        }

        Assert.assertEquals(expected[key], actual[key])
        orderProcessingApp.close()
    }

    class PropertyInit : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(applicationContext,
                    "perkss.kafka.example.bootstrap-servers=${kafka.bootstrapServers}",
                    "perkss.kafka.example.schema-registry=http://${schemaRegistry.containerIpAddress}:${schemaRegistry.getMappedPort(8081)}",
                    "perkss.kafka.example.state-dir=${TestUtils.tempDirectory().path}"
            )
        }

    }

}