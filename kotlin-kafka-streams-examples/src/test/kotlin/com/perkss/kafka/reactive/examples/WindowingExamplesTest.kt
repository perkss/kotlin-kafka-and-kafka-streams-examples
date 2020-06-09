package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde
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
     * Tumbling window we emit once per window.
     * Window start time set by: long windowStart = (Math.max(0, timestamp - sizeMs + advanceMs) / advanceMs) * advanceMs;
     * https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/kstream/TimeWindows.java#L175
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

        val outputTopic = testDriver.createOutputTopic(
                totalPostLengthPerUserTopic,
                TimeWindowedSerde(Serdes.String(), Duration.ofMinutes(2).toMillis()).deserializer(),
                Serdes.Long().deserializer())

        postCreatedTopic.pipeInput(aliceId, "Posting my first post", aliceFirstTimestamp)

        // Alice first post is length 21 and as this is Incremental Trigger updates are pushed each time
        val aliceOutputOne = outputTopic.readKeyValue()
        assertThat(aliceOutputOne.key.key(), equalTo(aliceId))
        assertThat(aliceOutputOne.value, equalTo(21L))
        assertWindowedResult(
                aliceOutputOne,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        postCreatedTopic.pipeInput(billId, "Bill posting my first post", billFirstTimestamp)

        // Bill first post
        val billOutputOne = outputTopic.readKeyValue()
        assertThat(billOutputOne.key.key(), equalTo(billId))
        assertThat(billOutputOne.value, equalTo(26L))
        assertWindowedResult(
                billOutputOne,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        postCreatedTopic.pipeInput(aliceId, "Alice sending her second post in the #1 window", aliceFirstTimestamp.plusSeconds(25))

        // Alice second post is in the same window so we total the post count
        val aliceOutputTwo = outputTopic.readKeyValue()
        assertThat(aliceOutputTwo.key.key(), equalTo(aliceId))
        assertThat(aliceOutputTwo.value, equalTo(67L))
        assertWindowedResult(
                aliceOutputTwo,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        postCreatedTopic.pipeInput(aliceId, "Posting my third post which is in window #2", aliceFirstTimestamp.plusSeconds(185))

        // Alice third post is in the next window so we have a new total post length count
        val aliceOutputThree = outputTopic.readKeyValue()
        assertThat(aliceOutputThree.key.key(), equalTo(aliceId))
        assertThat(aliceOutputThree.value, equalTo(43L))
        assertWindowedResult(
                aliceOutputThree,
                startWindow = LocalTime.of(20, 2, 0, 0),
                endWindow = LocalTime.of(20, 4, 0, 0))
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
                LocalTime.of(20, 1, 20, 0))
                .toInstant(ZoneOffset.UTC)

        val billFirstTimestamp = aliceFirstTimestamp.plusMillis(10)

        val outputTopic = testDriver.createOutputTopic(
                totalPostLengthPerUserTopic,
                TimeWindowedSerde(Serdes.String(), Duration.ofMinutes(2).toMillis()).deserializer(),
                Serdes.Long().deserializer())

        logger.info("Sending the first post from alice at {}", aliceFirstTimestamp)
        postCreatedTopic.pipeInput(aliceId, "Posting my first post", aliceFirstTimestamp)

        // Alice first post is length 21 and as this is Incremental Trigger updates are pushed each time
        // with the window always seemingly ending on the last full minute possible from event time when starting
        val aliceOutputOne = outputTopic.readKeyValue()
        assertThat(aliceOutputOne.key.key(), equalTo(aliceId))
        assertThat(aliceOutputOne.value, equalTo(21L))
        assertWindowedResult(
                aliceOutputOne,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        // window second emissions on hop
        val aliceOutputTwo = outputTopic.readKeyValue()
        assertThat(aliceOutputTwo.key.key(), equalTo(aliceId))
        assertThat(aliceOutputTwo.value, equalTo(21L))
        assertWindowedResult(
                aliceOutputTwo,
                startWindow = LocalTime.of(20, 1, 0, 0),
                endWindow = LocalTime.of(20, 3, 0, 0))

        postCreatedTopic.pipeInput(billId, "Bill posting my first post", billFirstTimestamp)

        // Bill first post
        val billOutputOne = outputTopic.readKeyValue()
        assertThat(billOutputOne.key.key(), equalTo(billId))
        assertThat(billOutputOne.value, equalTo(26L))
        assertWindowedResult(
                billOutputOne,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        // Bill first post hop second emission
        val billOutputTwo = outputTopic.readKeyValue()
        assertThat(billOutputTwo.key.key(), equalTo(billId))
        assertThat(billOutputTwo.value, equalTo(26L))
        assertWindowedResult(
                billOutputTwo,
                startWindow = LocalTime.of(20, 1, 0, 0),
                endWindow = LocalTime.of(20, 3, 0, 0))

        postCreatedTopic.pipeInput(aliceId, "Alice sending her second post in the #1 window", aliceFirstTimestamp.plusMillis(1))

        // Alice second post is in the same window so we total the post count
        val aliceOutputThree = outputTopic.readKeyValue()
        assertThat(aliceOutputThree.key.key(), equalTo(aliceId))
        assertThat(aliceOutputThree.value, equalTo(67L))
        assertWindowedResult(
                aliceOutputThree,
                startWindow = LocalTime.of(20, 0, 0, 0),
                endWindow = LocalTime.of(20, 2, 0, 0))

        val aliceOutputFour = outputTopic.readKeyValue()
        assertThat(aliceOutputFour.key.key(), equalTo(aliceId))
        assertThat(aliceOutputFour.value, equalTo(67L))
        assertWindowedResult(
                aliceOutputFour,
                startWindow = LocalTime.of(20, 1, 0, 0),
                endWindow = LocalTime.of(20, 3, 0, 0))

        postCreatedTopic.pipeInput(aliceId, "Posting my third post which is in window #2", aliceFirstTimestamp.plusSeconds(185))

        // Alice third post is in the next window so we have a new total post length count
        val aliceOutputFive = outputTopic.readKeyValue()
        assertThat(aliceOutputFive.key.key(), equalTo(aliceId))
        assertThat(aliceOutputFive.value, equalTo(43L))
        assertWindowedResult(
                aliceOutputFive,
                startWindow = LocalTime.of(20, 3, 0, 0),
                endWindow = LocalTime.of(20, 5, 0, 0))

        val aliceOutputSix = outputTopic.readKeyValue()
        assertThat(aliceOutputSix.key.key(), equalTo(aliceId))
        assertThat(aliceOutputSix.value, equalTo(43L))
        assertWindowedResult(
                aliceOutputSix,
                startWindow = LocalTime.of(20, 4, 0, 0),
                endWindow = LocalTime.of(20, 6, 0, 0))

        val billSecondTimestamp = billFirstTimestamp.plus(Duration.ofMinutes(6)).plusSeconds(15)
        logger.info("Sending the first post from alice at {}", billSecondTimestamp)

        // Bill post is in middle of two window at 2020-01-01T20:06:35.010Z
        postCreatedTopic.pipeInput(billId, "Bill posting my second post", billSecondTimestamp)

        // We output twice here due to windows
        val billOutputThree = outputTopic.readKeyValue()
        assertThat(billOutputThree.key.key(), equalTo(billId))
        assertThat(billOutputThree.value, equalTo(27L))
        assertWindowedResult(
                billOutputThree,
                startWindow = LocalTime.of(20, 6, 0, 0),
                endWindow = LocalTime.of(20, 8, 0, 0))

        // Second
        val billOutputFour = outputTopic.readKeyValue()
        assertThat(billOutputFour.key.key(), equalTo(billId))
        assertThat(billOutputFour.value, equalTo(27L))
        assertWindowedResult(
                billOutputFour,
                startWindow = LocalTime.of(20, 7, 0, 0),
                endWindow = LocalTime.of(20, 9, 0, 0))
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

        // https://github.com/confluentinc/kafka-streams-examples/blob/5.4.1-post/src/test/java/io/confluent/examples/streams/SessionWindowsExampleTest.java#L126

        // sent tombstones for the sessions that were merged retracted and updated basically
        val tombstone = outputTopic.readKeyValue()
        assertEquals(aliceId, tombstone.key)
        assertNull(tombstone.value)
        // Alice second post is in the same window so we total the post count
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

    private fun assertWindowedResult(
            outPut: KeyValue<Windowed<String>, Long>,
            startWindow: LocalTime,
            endWindow: LocalTime) {
        assertThat(outPut.key.window().startTime(), equalTo(
                LocalDateTime.of(
                        LocalDate.of(2020, 1, 1),
                        startWindow
                ).toInstant(ZoneOffset.UTC)))
        assertThat(outPut.key.window().endTime(), equalTo(
                LocalDateTime.of(
                        LocalDate.of(2020, 1, 1),
                        endWindow
                ).toInstant(ZoneOffset.UTC)))
    }

}