package com.perkss.kafka.reactive.examples

import com.perkss.social.media.model.PostCreated
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.slf4j.LoggerFactory
import java.time.Duration

object AggregateExamples {

    private val logger = LoggerFactory.getLogger(AggregateExamples::class.java)

    fun buildUserSocialMediaPostsTotalCountTopology(
        inputTopic: String,
        outputTopic: String,
        postCreatedSerde: SpecificAvroSerde<PostCreated>
    ): Topology {
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

    fun buildSocialMediaPostsWindowedTotalCountTopology(
        inputTopic: String,
        outputTopic: String,
        postCreatedSerde: SpecificAvroSerde<PostCreated>
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(
            inputTopic, Consumed.with(
                Serdes.String(), postCreatedSerde,
                PostCreatedTimestampExtractor, Topology.AutoOffsetReset.EARLIEST
            )
        )

        // Build a table of the counts
        val aggregated: KTable<Windowed<String>, Long> = input
            .peek { key, postCreated -> logger.info("Key {} and value {}", key, postCreated) }
            .groupBy { _, value -> value.userId } // group by the user who created the post
            .windowedBy(
                TimeWindows.of(Duration.ofSeconds(30))
            )
            .count()

        // stream the total counts per user keyed by user ID and the count
        aggregated.toStream { windowedKey, _ -> windowedKey.key() }
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }

    fun buildSocialMediaPostFinalWindowCountTopology(
        inputTopic: String,
        outputTopic: String,
        postCreatedSerde: SpecificAvroSerde<PostCreated>
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(
            inputTopic, Consumed.with(
                Serdes.String(), postCreatedSerde,
                PostCreatedTimestampExtractor, Topology.AutoOffsetReset.EARLIEST
            )
        )

        // Build a table of the counts
        val aggregated: KTable<Windowed<String>, Long> = input
            .peek { key, postCreated -> logger.info("Key {} and value {}", key, postCreated) }
            .groupBy { _, value -> value.userId } // group by the user who created the post
            .windowedBy(
                TimeWindows.of(Duration.ofSeconds(30)).grace(Duration.ZERO)
            )
            // Note we need to materialize here as serdes changes
            .count(Materialized.with(Serdes.String(), Serdes.Long()))
            // Suppress the output so only output of a window is emitted when the window closes.
            // Provide a name so it is constant in the KStream internally.
            .suppress(Suppressed.untilWindowCloses(unbounded()).withName("SocialMediaCountsSuppression"))

        // stream the total counts per user keyed by user ID and the count
        aggregated.toStream { windowedKey, _ -> windowedKey.key() }
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()))
        return builder.build()
    }
}