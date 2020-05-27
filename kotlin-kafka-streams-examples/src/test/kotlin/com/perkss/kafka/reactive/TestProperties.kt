package com.perkss.kafka.reactive

import io.confluent.common.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.*

object TestProperties {

    private val logger = LoggerFactory.getLogger(TestProperties::class.java)

    fun properties(appId: String,
                   broker: String,
                   defaultKeySerde: Class<out Serde<String>> = Serdes.String()::class.java,
                   defaultValSerde: Class<out Serde<String>> = Serdes.String()::class.java): Properties {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = appId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = broker
        props[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = defaultKeySerde
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = defaultValSerde
        val path = TestUtils.tempDirectory().path
        logger.info("Path for test directory is {}", path)
        props[StreamsConfig.STATE_DIR_CONFIG] = path
        props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 0
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return props
    }
}