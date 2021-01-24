package com.perkss.kafka.reactive

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.To
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*

class TimestampTransformer : Transformer<String, String, KeyValue<String, String>?> {

    private lateinit var context: ProcessorContext
    override fun init(context: ProcessorContext) {
        this.context = context
    }

    override fun close() {
    }

    override fun transform(key: String?, value: String?): KeyValue<String, String>? {
        // In reality use the timestamp on the event
        context.forward(
            key,
            value,
            To.all().withTimestamp(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli())
        )
        return null
    }
}


// Results in consuming only the latest message from the Topic
object BootstrapSemanticsSelfJoinTopology {

    private val logger = LoggerFactory.getLogger(BootstrapSemanticsSelfJoinTopology::class.java)

    fun build(streamsBuilder: StreamsBuilder, properties: Properties): Topology {
        val nameKTable = streamsBuilder
            .table(
                "name",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(IgnoreTimestampExtractor())
            )

        nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Stream Processing Key {}, Value {}", key, value)
            }
            .transform(
                { TimestampTransformer() })
            .peek { key, value ->
                logger.info("Changed Timestamp Key {}, Value {}", key, value)
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