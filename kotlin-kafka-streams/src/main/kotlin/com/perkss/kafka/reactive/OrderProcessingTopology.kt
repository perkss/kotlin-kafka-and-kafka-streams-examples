package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.model.toCustomer
import com.perkss.order.model.OrderRequested
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import java.util.*

fun stock(streamsBuilder: StreamsBuilder,
          props: AppProperties): KTable<String, String> =
        streamsBuilder.table(props.stockInventory, Consumed.with(Serdes.String(), Serdes.String()), Materialized.`as`(props.stockInventory))

fun customer(
        streamsBuilder: StreamsBuilder,
        props: AppProperties): GlobalKTable<String, GenericRecord> =
        streamsBuilder.globalTable(props.customerInformation, Consumed.with(Serdes.String(), GenericAvroSerde()), Materialized.`as`(props.customerInformation))


fun orderProcessing(streamConfig: Properties,
                    streamsBuilder: StreamsBuilder,
                    props: AppProperties,
                    customerTable: GlobalKTable<String, GenericRecord>,
                    stockTable: KTable<String, String>,
                    keySerde: Serde<String>,
                    valSerde: SpecificAvroSerde<OrderRequested>): Topology {
    streamsBuilder
            .stream(props.orderRequest, Consumed.with(keySerde, valSerde))
            .peek { key, value -> println(key + " " + value) }
            .leftJoin(stockTable, { leftValue: OrderRequested, rightValue: String? -> leftValue.productId })
            .peek { key, value -> println(key + " " + value) }
            .leftJoin(customerTable,
                    KeyValueMapper { leftKey: String, leftValue: String -> leftKey },
                    ValueJoiner { leftValue: String, rightValue: GenericRecord? -> rightValue?.toCustomer()?.name })
            .peek { key, value -> println(key + " " + value) }
            .mapValues { key, keyValue ->
                keyValue
            }
            .peek { key, value -> println(key + " " + value) }
            .to(props.outputTopic, Produced.with(keySerde, Serdes.String()))
    // pass to override for optimization
    return streamsBuilder.build(streamConfig)
}

