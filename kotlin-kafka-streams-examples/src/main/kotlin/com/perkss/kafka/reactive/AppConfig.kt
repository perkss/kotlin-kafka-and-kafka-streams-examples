package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.OrderProcessingTopology.customer
import com.perkss.kafka.reactive.OrderProcessingTopology.orderProcessing
import com.perkss.kafka.reactive.OrderProcessingTopology.stock
import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRejected
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
import org.springframework.beans.factory.annotation.Qualifier
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
        streamsConfiguration[StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG] =
            StreamsConfig.OPTIMIZE// do not create internal changelog have to have source topic as compact https://stackoverflow.com/questions/57164133/kafka-stream-topology-optimization
        // disable cache for testing
        streamsConfiguration[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java

        return streamsConfiguration
    }

    @Bean
    fun streamsBuilder() = StreamsBuilder()

    @Bean
    fun stockSerde(props: AppProperties): SpecificAvroSerde<Stock> =
        SpecificAvroSerde<Stock>().apply {
            configure(mutableMapOf(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry), false)
        }

    // stock table keyed by id of stock
    @Bean
    fun stockTable(
        streamsBuilder: StreamsBuilder,
        props: AppProperties,
        serde: SpecificAvroSerde<Stock>
    ): KTable<String, Stock> =
        stock(streamsBuilder, serde, props)

    // keyed by product ID
    @Bean
    fun customerTable(
        streamsBuilder: StreamsBuilder,
        props: AppProperties
    ): GlobalKTable<String, GenericRecord> =
        customer(streamsBuilder, props, Serdes.String(), GenericAvroSerde().apply {
            configure(
                mapOf(
                    KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                ), false
            ) // TODO make bean
        })

    @Bean
    fun orderProcessingTopology(
        @Qualifier("streamConfig")
        streamConfig: Properties,
        streamsBuilder: StreamsBuilder,
        props: AppProperties,
        customerTable: GlobalKTable<String, GenericRecord>,
        stockTable: KTable<String, Stock>,
        stockSerde: SpecificAvroSerde<Stock>
    ): Topology {
        return orderProcessing(
            streamConfig, streamsBuilder, props, customerTable, stockTable, Serdes.String(),
            SpecificAvroSerde<OrderRequested>().apply {
                configure(
                    mapOf(
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                    ), false
                )
            },
            SpecificAvroSerde<OrderRejected>().apply {
                configure(
                    mapOf(
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                    ), false
                )
            },
            SpecificAvroSerde<OrderConfirmed>().apply {
                configure(
                    mapOf(
                        KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry
                    ), false
                )
            },
            stockSerde
        )
    }

    @Bean
    fun orderProcessingApp(
        orderProcessingTopology: Topology,
        @Qualifier("streamConfig")
        streamConfig: Properties
    ) = KafkaStreams(orderProcessingTopology, streamConfig)

}
