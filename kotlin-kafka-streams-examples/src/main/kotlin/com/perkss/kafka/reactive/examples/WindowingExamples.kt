package com.perkss.kafka.reactive.examples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.SessionStore
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Toy Scenario where we window the length of posts by a user a running count and we can see how they behave differently
 * with Fixed, Tumbling and Session windows.
 * Assumptions Event Time on the message is the event time of the post
 */
class WindowingExamples {

    private val logger = LoggerFactory.getLogger(WindowingExamples::class.java)

    fun userLikesFixedWindowTopology(inputTopic: String,
                                     outputTopic: String,
                                     windowLengthMinutes: Long = 2,
                                     advanceMinutes: Long = 2): Topology {

        val builder = StreamsBuilder()

        // consume the user post by name and count
        val input = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek { key, value -> logger.info("Consuming Key {} value {}", key, value) }

        val windowSizeMs = Duration.ofMinutes(windowLengthMinutes)
        val advanceMs = Duration.ofMinutes(advanceMinutes)

        input
                .groupByKey() // Group by each user
                .windowedBy(TimeWindows.of(windowSizeMs).advanceBy(advanceMs).grace(Duration.ZERO))
                .aggregate(
                        { 0L },
                        { key, newValue, aggValue ->
                            logger.info("{} Adding {} to current total {}", key, newValue.length, aggValue)
                            aggValue + newValue.length
                        }, // We take the length of each post and aggregate them
                        Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                { windowedKey, _ -> windowedKey.key() }
                .peek { key, value -> logger.info("Publishing Key {} value {}", key, value) }
                // stream the total length of posts per window of each user
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }

    fun userPostLengthSessionWindowTopology(inputTopic: String,
                                            outputTopic: String,
                                            stateStoreName: String): Topology {
        val builder = StreamsBuilder()

        // consume the user like by name and count
        val input = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .peek { key, value -> logger.info("Consuming Key {} value {}", key, value) }

        val aggregatedUserPostLengths = input
                .groupByKey() // Group by each user
                .windowedBy(SessionWindows.with(Duration.ofMinutes(2)))
                .aggregate(
                        {
                            logger.info("Initializing")
                            0L
                        },
                        { _, newValue, aggValue ->
                            logger.info("Adding {} to current total {}", newValue.length, aggValue)
                            aggValue + newValue.length
                        }, // We take the length of each post and aggregate them
                        // Session merger
                        { _, leftAggValue, rightAggValue ->
                            leftAggValue + rightAggValue
                        },
                        Materialized.`as`<String, Long, SessionStore<Bytes, ByteArray>>(stateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                                .withLoggingDisabled())

        // stream the total length of posts per window of each user
        aggregatedUserPostLengths.toStream()
        { windowedKey, _ -> windowedKey.key() }
                .peek { key, value -> logger.info("Publishing Key {} value {}", key, value) }
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }
}