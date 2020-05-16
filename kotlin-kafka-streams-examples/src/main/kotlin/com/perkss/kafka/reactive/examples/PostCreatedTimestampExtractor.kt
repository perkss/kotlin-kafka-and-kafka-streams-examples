package com.perkss.kafka.reactive.examples

import com.perkss.social.media.model.PostCreated
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.LocalDateTime
import java.time.ZoneOffset

object PostCreatedTimestampExtractor : TimestampExtractor {

    override fun extract(record: ConsumerRecord<Any, Any>, previousTimestamp: Long): Long {
        return if (record.value() is PostCreated) {
            LocalDateTime.parse((record.value() as PostCreated).timestamp)
                    .toInstant(ZoneOffset.UTC).toEpochMilli()
        } else previousTimestamp
    }
}