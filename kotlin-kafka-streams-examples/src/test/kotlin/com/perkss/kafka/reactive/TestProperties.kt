package com.perkss.kafka.reactive

import io.confluent.common.utils.TestUtils
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object TestProperties {

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
        props[StreamsConfig.STATE_DIR_CONFIG] = TestUtils.tempDirectory().path
        props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
        props[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 1
        return props
    }
}