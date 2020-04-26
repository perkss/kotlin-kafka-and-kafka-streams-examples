package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.*
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
            streamsBuilder.table(props.stockInventory, Consumed.with(Serdes.String(), Serdes.String()))


    // keyed by product ID
    @Bean
    fun customerTable(
            streamsBuilder: StreamsBuilder,
            props: AppProperties): GlobalKTable<String, String> =
            streamsBuilder.globalTable(props.customerInformation, Consumed.with(Serdes.String(), Serdes.String()))

    @Bean
    fun orderProcessingTopology(
            streamConfig: Properties,
            streamsBuilder: StreamsBuilder,
            props: AppProperties,
            customerTable: GlobalKTable<String, String>,
            stockTable: KTable<String, String>): Topology {
        streamsBuilder
                .stream(props.orderRequest, Consumed.with(Serdes.String(), Serdes.String()))
                .leftJoin(stockTable) { leftValue: String, rightValue: String? -> leftValue + rightValue } // share same key
                .leftJoin(customerTable,
                        KeyValueMapper { leftKey: String, leftValue: String -> leftKey },
                        ValueJoiner { leftValue: String, rightValue: String? -> KeyValue.pair(leftValue, rightValue) })
                .mapValues { key, keyValue ->
                    keyValue.value
                }
                .to(props.outputTopic)
        // pass to override for optimization
        return streamsBuilder.build(streamConfig)
    }

    @Bean
    fun orderProcessingApp(orderProcessingTopology: Topology,
                           streamConfig: Properties) = KafkaStreams(orderProcessingTopology, streamConfig)

}