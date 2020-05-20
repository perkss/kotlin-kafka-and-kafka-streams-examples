package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class StreamingJoinExamplesTest {

    companion object {
        private val logger = LoggerFactory.getLogger(StreamingJoinExamplesTest::class.java)
    }

    private val firstNamesTopic = "first-names"
    private val lastNamesTopic = "last-names"
    private val fullNamesTopic = "full-names"
    private val props = TestProperties.properties("joining-examples", "test-host:9092")

    @Test
    fun `Users First name and Last name is joined with an inner streaming join`() {
        val innerJoinFullNamesTopology = StreamingJoinExamples.streamingInnerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val aliceId = "alice${UUID.randomUUID()}"
        val billId = "bill${UUID.randomUUID()}"
        val jasmineId = "jasmine${UUID.randomUUID()}"

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = Instant.now()

        // Alice is sent in the same window
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        val results = fullName.readKeyValuesToList()

        val expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }

        // Send in different windows on Inner Join

    }

    @Test
    fun `Users First name and Last name is joined with an left streaming join`() {
        val innerJoinFullNamesTopology = StreamingJoinExamples.streamingLeftJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val aliceId = "alice${UUID.randomUUID()}"
        val billId = "bill${UUID.randomUUID()}"
        val jasmineId = "jasmine${UUID.randomUUID()}"

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = Instant.now()

        // Alice is sent in the same window
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        val results = fullName.readKeyValuesToList()

        val expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }

        // Send in different windows on Inner Join

    }

    @Test
    fun `Users First name and Last name is joined with an outer streaming join`() {
        val innerJoinFullNamesTopology = StreamingJoinExamples.streamingOuterJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val aliceId = "alice${UUID.randomUUID()}"
        val billId = "bill${UUID.randomUUID()}"
        val jasmineId = "jasmine${UUID.randomUUID()}"

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = Instant.now()

        // Alice is sent in the same window
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        val results = fullName.readKeyValuesToList()

        val expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }

        // Send in different windows on Inner Join

    }

}