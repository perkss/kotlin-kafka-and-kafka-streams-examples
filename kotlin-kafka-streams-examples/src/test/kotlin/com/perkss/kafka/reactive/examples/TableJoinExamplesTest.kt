package com.perkss.kafka.reactive.examples

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset
import java.util.*

internal class TableJoinExamplesTest {
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
        val innerJoinFullNamesTopology = TableJoinExamples.innerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)

        // Alice is sent expect single out of full name
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice Parker")
        )

        kotlin.test.assertEquals(1, results.size)

        kotlin.test.assertTrue { results == expectedValues }

        // Inner join window is irrelevant for table table joins so emit
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        kotlin.test.assertEquals(1, results.size)

        // Inner Join so ordering is irrelevant emit only once from the table table join
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "Princess Jasmine")
        )

        kotlin.test.assertEquals(1, results.size)

        kotlin.test.assertTrue { results == expectedValues }
    }

    @Test
    fun `Users First name and Last name is joined with an left streaming join`() {
        val leftJoinFullNamesTopology = TableJoinExamples.leftJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(leftJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)

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

        kotlin.test.assertEquals(2, results.size)

        kotlin.test.assertTrue { results == expectedValues }

        // Table to Table Join so no windowing expect both sides to emit
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        kotlin.test.assertEquals(2, results.size)

        expectedValues = mutableListOf(
                KeyValue(billId, "Bill null"),
                KeyValue(billId, "Bill Preston")
        )

        kotlin.test.assertTrue { results == expectedValues }


        // Only emit when left side is there so right side is not emitted as comes first
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "Princess Jasmine")
        )

        kotlin.test.assertEquals(1, results.size)

        kotlin.test.assertTrue { results == expectedValues }
    }

    @Test
    fun `Users First name and Last name is joined with an outer streaming join`() {
        val outerJoinFullNamesTopology = TableJoinExamples.outerJoin(firstNamesTopic, lastNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(outerJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(firstNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val lastName = testDriver.createInputTopic(lastNamesTopic,
                Serdes.String().serializer(), Serdes.String().serializer())

        val startingTime = LocalDateTime.of(
                LocalDate.of(2020, 1, 1),
                LocalTime.of(20, 0, 0, 0))
                .toInstant(ZoneOffset.UTC)
        // outer join so emit on first event always
        firstName.pipeInput(aliceId, "Alice", startingTime)
        lastName.pipeInput(aliceId, "Parker", startingTime.plusSeconds(7))

        val fullName = testDriver.createOutputTopic(fullNamesTopic, Serdes.String().deserializer(),
                Serdes.String().deserializer())

        var results = fullName.readKeyValuesToList()

        var expectedValues = mutableListOf<KeyValue<String, String>>(
                KeyValue(aliceId, "Alice null"),
                KeyValue(aliceId, "Alice Parker")
        )

        kotlin.test.assertEquals(2, results.size)

        kotlin.test.assertTrue { results == expectedValues }

        // outer join so emit on first event always
        firstName.pipeInput(billId, "Bill", startingTime)
        lastName.pipeInput(billId, "Preston", startingTime.plusSeconds(12))

        results = fullName.readKeyValuesToList()

        kotlin.test.assertEquals(2, results.size)

        expectedValues = mutableListOf(
                KeyValue(billId, "Bill null"),
                KeyValue(billId, "Bill Preston")
        )

        kotlin.test.assertTrue { results == expectedValues }

        // Outer Join so always emit and side of join is irrelevant
        lastName.pipeInput(jasmineId, "Jasmine", startingTime)
        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
                KeyValue(jasmineId, "null Jasmine"),
                KeyValue(jasmineId, "Princess Jasmine")
        )

        kotlin.test.assertEquals(2, results.size)

        kotlin.test.assertTrue { results == expectedValues }
    }
}