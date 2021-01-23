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

object BootstrapSemanticsTopology {

    private val logger = LoggerFactory.getLogger(BootstrapSemanticsTopology::class.java)

    fun build(streamsBuilder: StreamsBuilder, properties: Properties): Topology {
        val nameKTable = streamsBuilder
            .table("name", Consumed.with(Serdes.String(), Serdes.String()))

        nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .selectKey { key, value ->
                val re = Regex("[^A-Za-z]")
                re.replace(value, "")
            }
            .join(nameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream Name {} to the KTable Name {}", value1, value2)
                value2
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("name-formatted", Produced.with(Serdes.String(), Serdes.String()))


        val topology = streamsBuilder.build(properties)
        logger.info("Bootstrap Topology Describe:\n {}", topology.describe())

        return topology
    }


}