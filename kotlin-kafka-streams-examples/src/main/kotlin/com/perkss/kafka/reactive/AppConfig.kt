package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.OrderProcessingTopology.customer
import com.perkss.kafka.reactive.OrderProcessingTopology.orderProcessing
import com.perkss.kafka.reactive.OrderProcessingTopology.stock
import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRequested
import com.perkss.order.model.Stock
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
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

    @Bean
    fun streamConfig(props: AppProperties): Properties {
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = props.applicationId
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = props.bootstrapServers
        streamsConfiguration[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = props.schemaRegistry
        streamsConfiguration[StreamsConfig.STATE_DIR_CONFIG] = props.stateDir
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
            props: AppProperties): KTable<String, Stock> =
            stock(streamsBuilder, props)

    // keyed by product ID
    @Bean
    fun customerTable(
            streamsBuilder: StreamsBuilder,
            props: AppProperties): GlobalKTable<String, GenericRecord> =
            customer(streamsBuilder, props, Serdes.String(), GenericAvroSerde().apply {
                configure(mapOf(
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                ), false) // TODO make bean
            })

    @Bean
    fun orderProcessingTopology(
            streamConfig: Properties,
            streamsBuilder: StreamsBuilder,
            props: AppProperties,
            customerTable: GlobalKTable<String, GenericRecord>,
            stockTable: KTable<String, Stock>): Topology {
        return orderProcessing(streamConfig, streamsBuilder, props, customerTable, stockTable, Serdes.String(),
                SpecificAvroSerde<OrderRequested>().apply {
                    configure(mapOf(
                            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                    ), false)
                },
                SpecificAvroSerde<OrderConfirmed>().apply {
                    configure(mapOf(
                            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                    ), false)
                })
    }

    @Bean
    fun orderProcessingApp(orderProcessingTopology: Topology,
                           streamConfig: Properties) = KafkaStreams(orderProcessingTopology, streamConfig)

}