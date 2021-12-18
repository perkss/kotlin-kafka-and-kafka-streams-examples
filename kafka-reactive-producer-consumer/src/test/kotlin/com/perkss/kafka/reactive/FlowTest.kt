package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.FlowTest.PropertyInit
import com.perkss.kafka.reactive.config.ReactiveKafkaAppProperties
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.support.TestPropertySourceUtils
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import reactor.core.publisher.Mono
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.SenderRecord
import reactor.test.test
import java.time.Duration


@SpringBootTest
@Testcontainers
@ContextConfiguration(initializers = [PropertyInit::class])
class FlowTest {

    @Autowired
    private lateinit var kafkaReactiveProducer: KafkaReactiveProducer<String, String>

    @Autowired
    private lateinit var reactiveKafkaAppProperties: ReactiveKafkaAppProperties

    companion object {
        lateinit var kafkaContainer: KafkaContainer

        @BeforeAll
        @JvmStatic
        internal fun beforeAll() {
            val imageName = DockerImageName.parse("confluentinc/cp-kafka:7.0.0")
            kafkaContainer = KafkaContainer(imageName)
            kafkaContainer.start()
            await until { kafkaContainer.isRunning }
        }
    }

    @Test
    fun `Sends a lowercase input string and then the topology converts it to uppercase string and outputs`() {

        val testConsumer = KafkaReactiveConsumer<String, String>(
            kafkaContainer.bootstrapServers,
            reactiveKafkaAppProperties.outputTopic,
            "test-consumer-group",
            "earliest"
        )

        val key = "1"
        val value = "pow"
        val producerRecord = ProducerRecord(reactiveKafkaAppProperties.inputTopic, key, value)

        val uppercaseMessages = mutableListOf<ReceiverRecord<String, String>>()

        val conf = mutableMapOf<String, Any>()
        conf[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        conf[AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG] = "5000"
        val adminClient = AdminClient.create(conf)

        val consumerGroups = adminClient.describeConsumerGroups(listOf(reactiveKafkaAppProperties.consumerGroupId))

        // send example message to topology
        kafkaReactiveProducer.send(
            Mono.just(SenderRecord.create(producerRecord, key))
        )
            .blockFirst()

        testConsumer.consume()
            .test()
            .expectSubscription()
            .recordWith { uppercaseMessages }
            .assertNext { uppercaseMessage ->
                assertEquals(value.toUpperCase(), uppercaseMessage.value())
                assertEquals(key, uppercaseMessage.key())
            }
            .expectNoEvent(Duration.ofSeconds(1))
            .thenCancel() // will infinitely consume to cancel
            .log()
            .verify()
    }

    class PropertyInit : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(applicationContext: ConfigurableApplicationContext) {
            TestPropertySourceUtils.addInlinedPropertiesToEnvironment(
                applicationContext,
                "perkss.kafka.example.bootstrap-servers=${kafkaContainer.bootstrapServers}"
            )
        }

    }

}