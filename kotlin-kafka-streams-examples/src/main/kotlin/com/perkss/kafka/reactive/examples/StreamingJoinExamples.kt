package com.perkss.kafka.reactive.examples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory
import java.time.Duration

object StreamingJoinExamples {

    private val logger = LoggerFactory.getLogger(StreamingJoinExamples::class.java)

    fun innerJoin(
        firstNamesTopic: String,
        lastNamesTopic: String,
        fullNameTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.stream(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.join(
            input2,
            { firstName, lastName -> "$firstName $lastName" },
            JoinWindows.of(Duration.ofSeconds(10))
        )


        // stream joined first and last names
        joined
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }

    fun leftJoin(
        firstNamesTopic: String,
        lastNamesTopic: String,
        fullNameTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.stream(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.leftJoin(
            input2,
            { firstName, lastName -> "$firstName $lastName" },
            JoinWindows.of(Duration.ofSeconds(10))
        )


        // stream joined first and last names
        joined
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }

    fun outerJoin(
        firstNamesTopic: String,
        lastNamesTopic: String,
        fullNameTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.stream(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.outerJoin(
            input2,
            { firstName, lastName -> "$firstName $lastName" },
            JoinWindows.of(Duration.ofSeconds(10))
        )


        // stream joined first and last names
        joined
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }

    fun outerJoinKTableToStream(
        firstNamesTopic: String,
        lastNamesTopic: String,
        fullNameTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.table(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input2.toStream().outerJoin(
            input,
            { lastName, firstName -> "$firstName $lastName" },
            JoinWindows.of(Duration.ofSeconds(10))
        )


        // stream joined first and last names
        joined
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }


}