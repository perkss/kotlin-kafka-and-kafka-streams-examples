package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import java.util.*

@EnableConfigurationProperties(AppProperties::class)
@Configuration
open class AppConfig {

    fun streamConfig(props: AppProperties): Properties {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = props.applicationId
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = props.bootstrapServers
        streamsConfiguration[StreamsConfig.STATE_DIR_CONFIG] = props.stateDir
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return streamsConfiguration
    }

    fun globalPayments(streamsBuilder: StreamsBuilder,
                       props: AppProperties): GlobalKTable<String, String> =
            streamsBuilder.globalTable(props.tableTopic, Consumed.with(Serdes.String(), Serdes.String()))

    fun paymentProcessor(streamsBuilder: StreamsBuilder,
                         props: AppProperties,
                         previousPaymentsTable: GlobalKTable<String, String>): KStream<String, String>? {
        return streamsBuilder
                .stream(props.inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .leftJoin(previousPaymentsTable,
                        KeyValueMapper { leftKey, leftValue -> leftKey },
                        ValueJoiner { leftValue, rightValue -> KeyValue.pair(leftValue, rightValue) })
                .mapValues { key, keyValue ->
                    keyValue.value
                }
    }
}