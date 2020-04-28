package com.perkss.kafka.reactive

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import java.util.*

fun stock(streamsBuilder: StreamsBuilder,
          props: AppProperties): KTable<String, String> =
        streamsBuilder.table(props.stockInventory, Consumed.with(Serdes.String(), Serdes.String()))

fun customer(
        streamsBuilder: StreamsBuilder,
        props: AppProperties): GlobalKTable<String, String> =
        streamsBuilder.globalTable(props.customerInformation, Consumed.with(Serdes.String(), Serdes.String()))


fun orderProcessingTopology(streamConfig: Properties,
                            streamsBuilder: StreamsBuilder,
                            props: AppProperties,
                            customerTable: GlobalKTable<String, String>,
                            stockTable: KTable<String, String>,
                            keySerde: Serde<String>,
                            valSerde: Serde<String>): Topology {
    streamsBuilder
            .stream(props.orderRequest, Consumed.with(keySerde, valSerde))
            .leftJoin(stockTable) { leftValue: String, rightValue: String? -> leftValue + rightValue }
            .leftJoin(customerTable,
                    KeyValueMapper { leftKey: String, leftValue: String -> leftKey },
                    ValueJoiner { leftValue: String, rightValue: String? -> rightValue })
            .mapValues { key, keyValue ->
                keyValue
            }
            .to(props.outputTopic)
    // pass to override for optimization
    return streamsBuilder.build(streamConfig)
}

