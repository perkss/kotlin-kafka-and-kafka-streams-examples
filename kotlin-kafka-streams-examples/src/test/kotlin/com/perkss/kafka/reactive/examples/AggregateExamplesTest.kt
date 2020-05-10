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
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*

internal class AggregateExamplesTest {

    private val schemaRegistryScope: String = AggregateExamplesTest::class.java.name
    private val mockSchemaRegistryUrl = "mock://$schemaRegistryScope"

    @Test
    fun buildUserSocialMediaPostsCountTopology() {
        val props = TestProperties.properties("aggregate-example-app", "test-host:9092")
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = mockSchemaRegistryUrl

        val inputTopicName = "post-created"
        val outputTopicName = "user-posts-total-count"

        val config = mapOf(
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to mockSchemaRegistryUrl)

        val postCreatedSerde = SpecificAvroSerde<PostCreated>()
        postCreatedSerde.configure(config, false)

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
        MockSchemaRegistry.dropScope(schemaRegistryScope)
    }
}