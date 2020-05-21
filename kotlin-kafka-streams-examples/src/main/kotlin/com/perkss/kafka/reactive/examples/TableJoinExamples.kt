package com.perkss.kafka.reactive.examples

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.slf4j.LoggerFactory

object TableJoinExamples {

    private val logger = LoggerFactory.getLogger(TableJoinExamples::class.java)

    fun innerJoin(firstNamesTopic: String,
                  lastNamesTopic: String,
                  fullNameTopic: String): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.table(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.table(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.join(input2) { firstName, lastName -> "$firstName $lastName" }

        // stream tables first and last names
        joined
                .toStream()
                .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
                .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }

    fun leftJoin(firstNamesTopic: String,
                 lastNamesTopic: String,
                 fullNameTopic: String): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.table(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.table(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.leftJoin(input2) { firstName, lastName -> "$firstName $lastName" }

        // stream tables first and last names
        joined
                .toStream()
                .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
                .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }

    fun outerJoin(firstNamesTopic: String,
                  lastNamesTopic: String,
                  fullNameTopic: String): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.table(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val input2 = builder.table(lastNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.outerJoin(input2) { firstName, lastName -> "$firstName $lastName" }

        // stream tables first and last names
        joined
                .toStream()
                .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
                .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }
}