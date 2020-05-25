package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.BeforeEach
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
    private lateinit var aliceId: String
    private lateinit var billId: String
    private lateinit var jasmineId: String

    @BeforeEach
    fun setup() {
        aliceId = "alice${UUID.randomUUID()}"
        jasmineId = "jasmine${UUID.randomUUID()}"
        billId = "bill${UUID.randomUUID()}"
    }

    @Test
    fun `Users First name and Last name is joined with an inner streaming join`() {
        val innerJoinFullNamesTopology = StreamingJoinExamples.innerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = Instant.now()

        // Alice is sent in the same window expect the single out of full name
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        // Inner join and out of the same window so no results expected
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        assertEquals(0, results.size)

        // Inner Join so ordering is irrelevant same window so output single value of joined name
        // Alice is sent in the same window expect the single out of full name
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "Princess Jasmine")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }
    }

    @Test
    fun `Users First name and Last name is joined with an left streaming join`() {
        val leftJoinFullNamesTopology = StreamingJoinExamples.leftJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(leftJoinFullNamesTopology, props)

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

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }

        // Different windows expect only first message to be emitted with null right side
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        assertEquals(1, results.size)

        expectedValues = mutableListOf(
                KeyValue(billId, "Bill null")
        )

        assertTrue { results == expectedValues }

        // Left Join so ordering is important expect single result as same window
        // Princess and Jasmine are sent in the same window expect the single out of full name
        // as right value comes first but is only emitted when left value arrives
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "Princess Jasmine")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }
    }

    @Test
    fun `Users First name and Last name is joined with an outer streaming join`() {
        val outerJoinFullNamesTopology = StreamingJoinExamples.outerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(outerJoinFullNamesTopology, props)

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

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }

        // Send in different windows on Inner Join
        // Different windows expect each message but no join as outside of same window
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        assertEquals(2, results.size)

        expectedValues = mutableListOf(
                KeyValue(billId, "Bill null"),
                KeyValue(billId, "null Preston")
        )

        assertTrue { results == expectedValues }

        // Outer Join so always emit and side of join is irrelevant
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "null Jasmine"),
                KeyValue(jasmineId, "Princess Jasmine")
        )

        assertEquals(2, results.size)

        assertTrue { results == expectedValues }
    }
}