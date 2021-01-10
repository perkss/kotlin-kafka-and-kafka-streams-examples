package com.perkss.kafka.reactive.processor

import com.perkss.kafka.reactive.TestProperties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.*

internal class JoinInMemoryStoreTest {

    private val firstNamesTopic = "first-names"
    private val fullNamesTopic = "full-names"
    private val props = TestProperties.properties("joining-examples", "test-host:9092")
    private lateinit var aliceId: String
    private lateinit var billId: String
    private lateinit var jasmineId: String

    @BeforeEach
    fun setup() {
        aliceId = "alice1"
        jasmineId = "jasmine2"
        billId = "bill3"
    }

    @Test
    fun `Users First name and Last name is joined with an inner streaming join`() {
        val innerJoinFullNamesTopology = JoinInMemoryStore.enrichFlowFromInMemoryStore(firstNamesTopic, fullNamesTopic)
        val testDriver = TopologyTestDriver(innerJoinFullNamesTopology, props)

        val firstName = testDriver.createInputTopic(
            firstNamesTopic,
            Serdes.String().serializer(), Serdes.String().serializer()
        )

        val startingTime = LocalDateTime.of(
            LocalDate.of(2020, 1, 1),
            LocalTime.of(20, 0, 0, 0)
        )
            .toInstant(ZoneOffset.UTC)

        // Alice is sent expect single out of full name
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

        firstName.pipeInput(billId, "Bill", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(billId, "Bill Preston")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(jasmineId, "Princess Jasmine")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        // We advance the wall clock time here to let the punctuator run and prepopulate the data
        testDriver.advanceWallClockTime(Duration.ofMinutes(6))
        // The the data again and get the punctuated updated values

        // Alice is sent expect single out of full name
        firstName.pipeInput(aliceId, "Alice", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf<KeyValue<String, String>>(
            KeyValue(aliceId, "Alice Parker 2")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        firstName.pipeInput(billId, "Bill", startingTime)

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(billId, "Bill Preston 2")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        firstName.pipeInput(jasmineId, "Princess", startingTime.plusSeconds(4))

        results = fullName.readKeyValuesToList()

        expectedValues = mutableListOf(
            KeyValue(jasmineId, "Princess Jasmine 2")
        )

        assertEquals(1, results.size)

        assertTrue { results == expectedValues }

        testDriver.close()
    }

}