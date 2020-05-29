package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import com.perkss.social.media.model.PostCreated
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*

class AggregateExamplesTest {

    companion object {
        private val logger = LoggerFactory.getLogger(AggregateExamplesTest::class.java)
    }

    private val schemaRegistryScope: String = AggregateExamplesTest::class.java.name
    private val mockSchemaRegistryUrl = "mock://$schemaRegistryScope"
    private val inputTopicName = "post-created"
    private val outputTopicName = "user-posts-total-count"

    private val config = mapOf(
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to mockSchemaRegistryUrl)

    private val props = TestProperties.properties("aggregate-example-app", "test-host:9092")

    private val postCreatedSerde = SpecificAvroSerde<PostCreated>()

    @BeforeEach
    fun setup() {
        postCreatedSerde.configure(config, false)
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = mockSchemaRegistryUrl
    }

    @AfterEach
    fun teardown() {
        MockSchemaRegistry.dropScope(schemaRegistryScope)
    }

    @Test
    fun `Social Media Post total count no windowing per user`() {
        val totalUserSocialMediaPostsTopology = AggregateExamples.buildUserSocialMediaPostsTotalCountTopology(inputTopicName, outputTopicName, postCreatedSerde)
        val testDriver = TopologyTestDriver(totalUserSocialMediaPostsTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(inputTopicName,
                Serdes.String().serializer(), postCreatedSerde.serializer())

        val alice = UUID.randomUUID().toString()
        val bill = UUID.randomUUID().toString()
        val jasmine = UUID.randomUUID().toString()

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(), PostCreated(UUID.randomUUID().toString(), alice, "Happy", LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)))
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(), PostCreated(UUID.randomUUID().toString(), bill, "Party", LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)))
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(), PostCreated(UUID.randomUUID().toString(), alice, "Running", LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)))
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(), PostCreated(UUID.randomUUID().toString(), jasmine, "Drinking", LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)))
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(), PostCreated(UUID.randomUUID().toString(), alice, "Travelling", LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)))

        val outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        // Alice has a single post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(bill, 1L)))
        // Alice has a second post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 2L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(jasmine, 1L)))
        // Alice has a third post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 3L)))

        testDriver.close()
    }

    @Test
    fun `Social Media Post with windows emit each event and total the number of posts per window by user`() {
        val totalUserSocialMediaPostsTopology = AggregateExamples.buildSocialMediaPostsWindowedTotalCountTopology(inputTopicName, outputTopicName, postCreatedSerde)
        val testDriver = TopologyTestDriver(totalUserSocialMediaPostsTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(inputTopicName,
                Serdes.String().serializer(), postCreatedSerde.serializer())

        val alice = "alice${UUID.randomUUID()}"
        val bill = "bill${UUID.randomUUID()}"
        val jasmine = "jasmine${UUID.randomUUID()}"

        val eventTimeStamp1 = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)
        // #Window 1
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(),
                        alice,
                        "Happy",
                        eventTimeStamp1.formatInstantToIsoDateTime()))

        // Move 20 seconds forward in time #Window 1
        val eventTimeStamp2 = eventTimeStamp1.plusSeconds(20)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(
                        UUID.randomUUID().toString(),
                        bill,
                        "Party",
                        eventTimeStamp2.formatInstantToIsoDateTime()))

        // Move 20 seconds forward in time #Window 1
        val eventTimeStamp3 = eventTimeStamp2.plusSeconds(20)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(
                        UUID.randomUUID().toString(),
                        alice,
                        "Running",
                        eventTimeStamp3.formatInstantToIsoDateTime()))

        // New window
        // Move 2 seconds forward in time #Window 2
        val eventTimeStamp4 = eventTimeStamp3.plusSeconds(2)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(), jasmine, "Drinking",
                        eventTimeStamp4.formatInstantToIsoDateTime()))

        // Move 1 seconds forward in time #Window 2
        val eventTimeStamp5 = eventTimeStamp4.plusSeconds(1)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(), alice, "Travelling",
                        eventTimeStamp5.formatInstantToIsoDateTime()))

        val outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        // Alice has a single post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(bill, 1L)))
        // # Window 2 Alice has a second post so its emitted as count 1
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 1L)))
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(jasmine, 1L)))
        // Alice has a third post which is in the same window as her second post so aggregate
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(alice, 2L)))

        testDriver.close()
    }

    @Test
    fun `Social media post count emitted in suppressed window so only final window count is emitted`() {
        val totalUserSocialMediaPostsTopology = AggregateExamples.buildSocialMediaPostFinalWindowCountTopology(inputTopicName, outputTopicName, postCreatedSerde)
        val testDriver = TopologyTestDriver(totalUserSocialMediaPostsTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(inputTopicName,
                Serdes.String().serializer(), postCreatedSerde.serializer())

        val alice = "alice${UUID.randomUUID()}"
        val bill = "bill${UUID.randomUUID()}"
        val jasmine = "jasmine${UUID.randomUUID()}"

        val eventTimeStamp1 = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)
        // #Window 1
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(),
                        alice,
                        "Happy",
                        eventTimeStamp1.formatInstantToIsoDateTime()))

        // Move 10 seconds forward in time #Window 1
        val eventTimeStamp2 = eventTimeStamp1.plusSeconds(10)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(
                        UUID.randomUUID().toString(),
                        bill,
                        "Party",
                        eventTimeStamp2.formatInstantToIsoDateTime()))

        // New window
        // Move 40 seconds forward in time #Window 2
        val eventTimeStamp3 = eventTimeStamp2.plusSeconds(40)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(
                        UUID.randomUUID().toString(),
                        alice,
                        "Running",
                        eventTimeStamp3.formatInstantToIsoDateTime()))

        // Move 2 seconds forward in time #Window 2
        val eventTimeStamp4 = eventTimeStamp3.plusSeconds(2)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(), jasmine, "Drinking",
                        eventTimeStamp4.formatInstantToIsoDateTime()))

        // Move 1 seconds forward in time #Window 2
        val eventTimeStamp5 = eventTimeStamp4.plusSeconds(1)

        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(), alice, "Travelling",
                        eventTimeStamp5.formatInstantToIsoDateTime()))

        val eventTimeStamp6 = eventTimeStamp5.plusSeconds(45)
        // Dummy event to close the window suppress() will only emit if event-time passed window-end time plus grace-period
        // so we need to let the stream processor know that the window has closed now due to a future event time entering the system
        logger.info("Sending in dummy event to flush the window")
        postCreatedTopic.pipeInput(UUID.randomUUID().toString(),
                PostCreated(UUID.randomUUID().toString(), alice, "Close that window",
                        eventTimeStamp6.formatInstantToIsoDateTime()))

        val outputTopic = testDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        val expectedValues = mutableListOf<KeyValue<String, Long>>(
                KeyValue(alice, 1L), // #Window1 Alice has a single post
                KeyValue(bill, 1L), // #Window1
                KeyValue(alice, 2L), // #Window2 Alice has a second post and a third post in the same window and it only emits the final values
                KeyValue(jasmine, 1L) // #Window2
        )

        assertTrue { outputTopic.readKeyValuesToList() == expectedValues }
    }

    private fun Instant.formatInstantToIsoDateTime(): String =
            LocalDateTime.ofInstant(this, ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME)

}