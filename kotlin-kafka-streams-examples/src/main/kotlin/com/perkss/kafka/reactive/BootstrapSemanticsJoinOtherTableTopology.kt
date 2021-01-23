package com.perkss.kafka.reactive

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Joined
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueJoiner
import org.slf4j.LoggerFactory
import java.util.*

object BootstrapSemanticsJoinOtherTableTopology {

    private val logger = LoggerFactory.getLogger(BootstrapSemanticsJoinOtherTableTopology::class.java)

    fun build(streamsBuilder: StreamsBuilder, properties: Properties): Topology {
        val nameKTable = streamsBuilder
            .table("first-name", Consumed.with(Serdes.String(), Serdes.String()))

        val lastNameKTable = streamsBuilder
            .table("last-name", Consumed.with(Serdes.String(), Serdes.String()))

        nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .join(lastNameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream First Name {} to the KTable Last Name {}", value1, value2)
                "$value1 $value2"
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("joined-name", Produced.with(Serdes.String(), Serdes.String()))


        val topology = streamsBuilder.build(properties)
        logger.info("Bootstrap Topology Describe:\n {}", topology.describe())

        return topology
    }


}