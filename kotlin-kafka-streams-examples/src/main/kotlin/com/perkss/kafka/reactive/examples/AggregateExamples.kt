package com.perkss.kafka.reactive.examples

import com.perkss.social.media.model.PostCreated
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.TimestampExtractor
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneOffset

object AggregateExamples {

    private val logger = LoggerFactory.getLogger(AggregateExamples::class.java)

    fun buildUserSocialMediaPostsTotalCountTopology(inputTopic: String,
                                                    outputTopic: String,
                                                    postCreatedSerde: SpecificAvroSerde<PostCreated>): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(inputTopic, Consumed.with(Serdes.String(), postCreatedSerde))

        // Build a table of the counts
        val aggregated: KTable<String, Long> = input
                .groupBy { _, value -> value.userId } // group by the user who created the post
                .count()
        // stream the total counts per user keyed by user ID and the count
        aggregated.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }

    fun buildUserSocialMediaPostsWindowedTotalCountTopology(inputTopic: String,
                                                            outputTopic: String,
                                                            postCreatedSerde: SpecificAvroSerde<PostCreated>): Topology {
        // custom timestamp extractor
        val timestampExtractor = TimestampExtractor { record: ConsumerRecord<Any, Any>, previousTimestamp: Long ->
            if (record.value() is PostCreated) {
                LocalDateTime.parse((record.value() as PostCreated).timestamp)
                        .toInstant(ZoneOffset.UTC).toEpochMilli()
            } else previousTimestamp
        }

        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(inputTopic, Consumed.with(Serdes.String(), postCreatedSerde,
                timestampExtractor, Topology.AutoOffsetReset.EARLIEST))

        // Build a table of the counts
        val aggregated: KTable<Windowed<String>, Long> = input
                .peek { key, postCreated -> logger.info("Key {} and value {}", key, postCreated) }
                .groupBy { _, value -> value.userId } // group by the user who created the post
                .windowedBy(
                        TimeWindows.of(Duration.ofSeconds(30)))
                .count()

        // stream the total counts per user keyed by user ID and the count
        aggregated.toStream { windowedKey, _ -> windowedKey.key() }
                .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }

}