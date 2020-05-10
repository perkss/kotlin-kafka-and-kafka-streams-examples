package com.perkss.kafka.reactive.examples

import com.perkss.social.media.model.PostCreated
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Produced

object AggregateExamples {

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

}