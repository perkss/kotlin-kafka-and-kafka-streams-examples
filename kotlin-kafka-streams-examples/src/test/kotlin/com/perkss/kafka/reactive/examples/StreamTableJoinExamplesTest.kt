package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.*

internal class StreamTableJoinExamplesTest {

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
        val innerJoinFullNamesTopology =
            StreamTableJoinExamples.innerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(
            firstNamesTopic,
            Serdes.String().serializer(), Serdes.String().serializer()
        )

        val lastName = testDriver.createInputTopic(
            lastNamesTopic,
            Serdes.String().serializer(), Serdes.String().serializer()
        )

        val startingTime = LocalDateTime.of(
            LocalDate.of(2020, 1, 1),
            LocalTime.of(20, 0, 0, 0)
        )
            .toInstant(ZoneOffset.UTC)

        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))
        // Alice is sent in the same window expect the single out of full name
        firstName.pipeInput(aliceId, "Alice", startingTime)

        val fullName = testDriver.createOutputTopic(
            fullNamesTopic, Serdes.String().deserializer(),
            Serdes.String().deserializer()
        )

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
            KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))
        // Inner join as Streaming Table Join window is irrelevant
        firstName.pipeInput(billId, "Bill", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(billId, "Bill Preston")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))
        // Inner Join is empty as the Table update comes after the Streaming left side and inner join only emits if
        // if both are there and does not emit when table side arrives
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(jasmineId, "Princess Jasmine")
        )

        assertEquals(0, results.size)

    }

    @Test
    fun `Users First name and Last name is joined with an left streaming join`() {
        val leftJoinFullNamesTopology =
            StreamTableJoinExamples.leftJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(leftJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(
            firstNamesTopic,
            Serdes.String().serializer(), Serdes.String().serializer()
        )

        val lastName = testDriver.createInputTopic(
            lastNamesTopic,
            Serdes.String().serializer(), Serdes.String().serializer()
        )

        val startingTime = LocalDateTime.of(
            LocalDate.of(2020, 1, 1),
            LocalTime.of(20, 0, 0, 0)
        )
            .toInstant(ZoneOffset.UTC)

        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))
        // Alice is sent in the same window
        firstName.pipeInput(aliceId, "Alice", startingTime)

        val fullName = testDriver.createOutputTopic(
            fullNamesTopic, Serdes.String().deserializer(),
            Serdes.String().deserializer()
        )

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
            KeyValue(aliceId, "Alice Parker")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))
        // Different windows expect only first message to be emitted with null right side
        firstName.pipeInput(billId, "Bill", startingTime)

        results = fullName.readKeyValuesToList()

        assertEquals(1, results.size)

        expectedValues = mutableListOf(
            KeyValue(billId, "Bill Preston")
        )

        assertTrue { results == expectedValues }

        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))
        // Left Join so ordering is important table value arrives after stream value on the left side of the
        // join so it will emit null for the right side value last name.
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(jasmineId, "Princess null")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }
    }
}