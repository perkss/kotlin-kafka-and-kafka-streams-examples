package com.perkss.kafka.reactive

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.*
import java.util.*

// TODO only pass the required prop
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
                            stockTable: KTable<String, String>): Topology {
    streamsBuilder
            .stream(props.orderRequest, Consumed.with(Serdes.String(), Serdes.String()))
            .leftJoin(stockTable) { leftValue: String, rightValue: String? -> leftValue + rightValue } // share same key
            .leftJoin(customerTable,
                    KeyValueMapper { leftKey: String, leftValue: String -> leftKey },
                    ValueJoiner
                    { leftValue: String, rightValue: String? -> KeyValue.pair(leftValue, rightValue) }
            )
            .mapValues { key, keyValue ->
                keyValue.value
            }
            .to(props.outputTopic)
    // pass to override for optimization
    return streamsBuilder.build(streamConfig)
}

