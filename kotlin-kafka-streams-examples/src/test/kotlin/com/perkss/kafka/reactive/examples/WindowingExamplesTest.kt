package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.*
import java.util.*

internal class WindowingExamplesTest {

    companion object {
        private val logger = LoggerFactory.getLogger(WindowingExamples::class.java)
    }

    private val postTopic = "user-posts"
    private val totalPostLengthPerUserTopic = "total-post-length-per-user"
    private val props = TestProperties.properties("windowing-examples", "test-host:9092")
    private lateinit var windowingExamples: WindowingExamples
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var aliceId: String
    private lateinit var billId: String
    private lateinit var jasmineId: String

    @BeforeEach
    fun setup() {
        windowingExamples = WindowingExamples()
        aliceId = "alice${UUID.randomUUID()}"
        jasmineId = "jasmine${UUID.randomUUID()}"
        billId = "bill${UUID.randomUUID()}"
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }

    /**
     * Tumbling window we emit once per window
     */
    @Test
    fun `Fixed Size Tumbling Window of Social Media Post Length`() {
        val postLengthTopology = windowingExamples
                .userLikesFixedWindowTopology(postTopic, totalPostLengthPerUserTopic,
                        2, 2)
        testDriver = TopologyTestDriver(postLengthTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(postTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val aliceFirstTimestamp = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)
        val billFirstTimestamp = aliceFirstTimestamp.plusMillis(10)

        val outputTopic = testDriver.createOutputTopic(totalPostLengthPerUserTopic, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        postCreatedTopic.pipeInput(aliceId, "Posting my first post", aliceFirstTimestamp)

        // Alice first post is length 21 and as this is Incremental Trigger updates are pushed each time
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 21L)))

        postCreatedTopic.pipeInput(billId, "Bill posting my first post", billFirstTimestamp)

        // Bill first post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(billId, 26L)))

        postCreatedTopic.pipeInput(aliceId, "Alice sending her second post in the #1 window", aliceFirstTimestamp.plusSeconds(25))

        // Alice second post is in the same window so we total the post count
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 67L)))

        postCreatedTopic.pipeInput(aliceId, "Posting my third post which is in window #2", aliceFirstTimestamp.plusSeconds(185))

        // Alice third post is in the next window so we have a new total post length count
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 43L)))
    }

    /**
     * Hopping window we can emit multiple times in the window so window is on the Minutes of
     * for example [2020-01-01T20:05:00Z 2020-01-01T20:07:00Z] and [2020-01-01T20:06:00Z, 2020-01-01T20:08:00Z]
     * https://www.confluent.io/blog/watermarks-tables-event-time-dataflow-model/
     */
    @Test
    fun `Hopping Window of 2 minutes with 1 minute overlaps of Social Media Post Length`() {
        val postLengthTopology = windowingExamples
                .userLikesFixedWindowTopology(
                        postTopic,
                        totalPostLengthPerUserTopic,
                        2,
                        1)

        testDriver = TopologyTestDriver(postLengthTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(postTopic,
                Serdes.String().serializer(), Serdes.String().serializer())
        //https://stackoverflow.com/questions/43771904/kafka-streams-hopping-windows-deduplicate-keys
        val aliceFirstTimestamp = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 20, 0))
                .toInstant(ZoneOffset.UTC)
        val billFirstTimestamp = aliceFirstTimestamp.plusMillis(10)

        val outputTopic = testDriver.createOutputTopic(totalPostLengthPerUserTopic, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        logger.info("Sending the first post from alice at {}", aliceFirstTimestamp)
        postCreatedTopic.pipeInput(aliceId, "Posting my first post", aliceFirstTimestamp)

        // Alice first post is length 21 and as this is Incremental Trigger updates are pushed each time
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 21L)))

        // window second emissions on hop
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 21L)))

        postCreatedTopic.pipeInput(billId, "Bill posting my first post", billFirstTimestamp)

        // Bill first post
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(billId, 26L)))

        // Bill first post hop second emission
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(billId, 26L)))

        postCreatedTopic.pipeInput(aliceId, "Alice sending her second post in the #1 window", aliceFirstTimestamp.plusMillis(1))

        // Alice second post is in the same window so we total the post count
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 67L)))

        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 67L)))

        postCreatedTopic.pipeInput(aliceId, "Posting my third post which is in window #2", aliceFirstTimestamp.plusSeconds(185))

        // Alice third post is in the next window so we have a new total post length count
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 43L)))

        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 43L)))

        val billSecondTimestamp = billFirstTimestamp.plus(Duration.ofMinutes(6)).plusSeconds(15)
        logger.info("Sending the first post from alice at {}", billSecondTimestamp)

        // Bill post is in middle of two window at 2020-01-01T20:06:35.010Z
        postCreatedTopic.pipeInput(billId, "Bill posting my second post", billSecondTimestamp)

        // We output twice here due to windows First [2020-01-01T20:05:00Z 2020-01-01T20:07:00Z]
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(billId, 27L)))

        // Second is [2020-01-01T20:06:00Z, 2020-01-01T20:08:00Z]
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(billId, 27L)))
    }

    @Test
    fun `Session window of Social Media Post Length`() {
        val stateStoreName = "SessionWindowStore"

        val postLengthTopology = windowingExamples
                .userPostLengthSessionWindowTopology(postTopic, totalPostLengthPerUserTopic, stateStoreName)
        testDriver = TopologyTestDriver(postLengthTopology, props)

        val postCreatedTopic = testDriver.createInputTopic(postTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val aliceFirstTimestamp = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)

        val outputTopic: TestOutputTopic<String, Long> = testDriver.createOutputTopic(totalPostLengthPerUserTopic, Serdes.String().deserializer(),
                Serdes.Long().deserializer())

        logger.info("Sending on first post")
        postCreatedTopic.pipeInput(aliceId, "Posting my first post", aliceFirstTimestamp)

        // Alice first post is length 21 and as this is Incremental Trigger updates are pushed each time
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 21L)))

        logger.info("Sending on second post")
        postCreatedTopic.pipeInput(aliceId, "Alice sending her second post in the #1 window", aliceFirstTimestamp.plusSeconds(55))

        // Alice second post is in the same window so we total the post count
        // https://github.com/confluentinc/kafka-streams-examples/blob/5.4.1-post/src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java#L126

        // sent tombstones for the sessions that were merged retracted and updated basically
        val tombstone = outputTopic.readKeyValue()
        assertEquals(aliceId, tombstone.key)
        assertNull(tombstone.value)

        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 67L)))

        logger.info("Sending on third post")
        postCreatedTopic.pipeInput(aliceId, "Posting my third post which is in window #2", aliceFirstTimestamp.plusSeconds(165))

        // Update Tombstone once third post sent
        val tombstone2 = outputTopic.readKeyValue()
        assertEquals(aliceId, tombstone2.key)
        assertNull(tombstone2.value)
        // Alice third post is in the same session window
        assertThat(outputTopic.readKeyValue(), equalTo(KeyValue(aliceId, 110L)))
    }

}