package com.perkss.kafka.reactive

import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRequested
import com.perkss.order.model.Stock
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import org.slf4j.LoggerFactory
import java.util.*

object OrderProcessingTopology {
    private val logger = LoggerFactory.getLogger(OrderProcessingTopology::class.java)

    fun stock(streamsBuilder: StreamsBuilder,
              props: AppProperties): KTable<String, Stock> {
        val specificAvroSerde = SpecificAvroSerde<Stock>().apply {
            configure(mutableMapOf(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to props.schemaRegistry), false)
        }
        return streamsBuilder.table(props.stockInventory, Consumed.with(Serdes.String(), specificAvroSerde))
        //, Materialized.`as`(props.stockInventory)
    }


    fun customer(streamsBuilder: StreamsBuilder,
                 props: AppProperties,
                 keySerde: Serde<String>,
                 valueSerde: GenericAvroSerde): GlobalKTable<String, GenericRecord> =
            streamsBuilder
                    .globalTable(props.customerInformation,
                            Consumed.with(keySerde, valueSerde), Materialized.`as`(props.customerInformation))

    fun orderProcessing(streamConfig: Properties,
                        streamsBuilder: StreamsBuilder,
                        props: AppProperties,
                        customerTable: GlobalKTable<String, GenericRecord>,
                        stockTable: KTable<String, Stock>,
                        keySerde: Serde<String>,
                        valSerde: SpecificAvroSerde<OrderRequested>,
                        orderConfirmedSerde: SpecificAvroSerde<OrderConfirmed>): Topology {
        streamsBuilder
                .stream(props.orderRequest, Consumed.with(keySerde, valSerde))
                .peek { key, value -> logger.info("Consumed {} {}", key, value) }
                .leftJoin(stockTable) { leftValue: OrderRequested, rightValue: Stock? ->
                    if (rightValue != null && rightValue.quantityAvailable > 0) {
                        leftValue
                    } else {
                        leftValue // TOOD branch the stream and send not filled response
                    }
                }
                .peek { key, value -> logger.info("Joined with Stock {} {}", key, value) }
                .leftJoin(customerTable,
                        // Foreign Key join allowed as a GlobalKTable
                        KeyValueMapper { leftKey: String, leftValue: OrderRequested -> leftValue.customerId },
                        ValueJoiner { leftValue: OrderRequested, rightValue: GenericRecord? ->
                            if (rightValue != null) {
                                OrderConfirmed(leftValue.id, leftValue.productId, leftValue.customerId, true)
                            } else {
                                logger.warn("No customer found.")
                                null
                            }
                        })
                .filter { _, value -> value != null } // TODO is this needed
                .peek { key, value -> logger.info("Joined with customer {} {}", key, value) }
                .to(props.orderProcessedTopic, Produced.with(keySerde, orderConfirmedSerde))
        // pass to override for optimization
        return streamsBuilder.build(streamConfig)
    }
}


