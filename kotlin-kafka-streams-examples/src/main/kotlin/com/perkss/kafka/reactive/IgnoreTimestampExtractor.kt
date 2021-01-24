package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor

class IgnoreTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>?, partitionTime: Long): Long {
        // Ignore the timestamp to enable a KTable to act like a global KTable and bootstrap fully
        // before processing it against joins.
        return 0
    }
}