package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.kstream.KTable
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

@EnableConfigurationProperties(AppProperties::class)
@Configuration
class AppConfig {

    // input topic order request // keyed by product id
    // join with Stock on ktable product ID key
    // join with customerId and information globalktable foreign key
    @Bean
    fun streamConfig(props: AppProperties): Properties {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = props.applicationId
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = props.bootstrapServers
//        streamsConfiguration[StreamsConfig.STATE_DIR_CONFIG] = props.stateDir
        streamsConfiguration[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        streamsConfiguration[StreamsConfig.TOPOLOGY_OPTIMIZATION] = StreamsConfig.OPTIMIZE// do not create internal changelog have to have source topic as compact https://stackoverflow.com/questions/57164133/kafka-stream-topology-optimization
        return streamsConfiguration
    }

    @Bean
    fun streamsBuilder() = StreamsBuilder()

    // stock table keyed by id of stock
    @Bean
    fun stockTable(
            streamsBuilder: StreamsBuilder,
            props: AppProperties): KTable<String, String> =
            stock(streamsBuilder, props)


    // keyed by product ID
    @Bean
    fun customerTable(
            streamsBuilder: StreamsBuilder,
            props: AppProperties): GlobalKTable<String, String> =
            customer(streamsBuilder, props)

    @Bean
    fun orderProcessingTopology(
            streamConfig: Properties,
            streamsBuilder: StreamsBuilder,
            props: AppProperties,
            customerTable: GlobalKTable<String, String>,
            stockTable: KTable<String, String>): Topology {
        return orderProcessingTopology(streamConfig, streamsBuilder, props, customerTable, stockTable)
    }

    @Bean
    fun orderProcessingApp(orderProcessingTopology: Topology,
                           streamConfig: Properties) = KafkaStreams(orderProcessingTopology, streamConfig)

}