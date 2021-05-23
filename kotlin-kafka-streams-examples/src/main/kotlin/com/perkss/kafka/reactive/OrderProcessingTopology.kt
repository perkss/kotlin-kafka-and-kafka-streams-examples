package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.processor.RekeyStream
import com.perkss.order.model.OrderConfirmed
import com.perkss.order.model.OrderRejected
import com.perkss.order.model.OrderRequested
import com.perkss.order.model.Stock
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
              stockSerde: SpecificAvroSerde<Stock>,
              props: AppProperties): KTable<String, Stock> {
        return streamsBuilder.table(props.stockInventory, Consumed.with(Serdes.String(), stockSerde))
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
                        orderRequestedSerde: SpecificAvroSerde<OrderRequested>,
                        orderRejectedSerde: SpecificAvroSerde<OrderRejected>,
                        orderConfirmedSerde: SpecificAvroSerde<OrderConfirmed>,
                        stockSerde: SpecificAvroSerde<Stock>): Topology {

        val rekeyStream = { RekeyStream<String, OrderRequested> { it.productId } }

        val split = streamsBuilder
            .stream(props.orderRequest, Consumed.with(keySerde, orderRequestedSerde))
            .peek { key, value -> logger.info("Consumed {} {}", key, value) }
            // Rekey to the Product ID so we can join with Product Table
            //.selectKey { _, value -> value.productId }
            // Use the Processor API to rekey so we do dont create another topic
            .transform(rekeyStream)
            .peek { key, value -> logger.info("Rekeyed to Product Id {} {}", key, value) }
            .leftJoin(stockTable, { orderRequest: OrderRequested, stock: Stock? ->
                    if (stock != null && stock.quantityAvailable > 0) {
                        orderRequest
                    } else {
                        OrderRejected(UUID.randomUUID().toString(), orderRequest.id, "Not enough stock")
                    }
                }, Joined.with(keySerde, orderRequestedSerde, stockSerde))
                .peek { key, value -> logger.info("Joined with Stock {} {}", key, value) }
                .branch(Predicate<String, Any> { key, value -> value is OrderRequested },
                        Predicate<String, Any> { key, value -> value is OrderRejected })

        // Order Requested Flow
        split[0]
                .mapValues { value -> value as OrderRequested }
                .selectKey { _, value -> value.id }
                .peek { key, orderRequested ->
                    logger.info("Order Requested {} for customer {}",
                            key, orderRequested.customerId)
                }
                .leftJoin(customerTable,
                        // Foreign Key join allowed as a GlobalKTable
                        KeyValueMapper<String, OrderRequested, String> { _: String, orderRequest: OrderRequested -> orderRequest.customerId },
                        ValueJoiner<OrderRequested, GenericRecord, OrderConfirmed?> { leftValue: OrderRequested, rightValue: GenericRecord? ->
                            if (rightValue != null) {
                                OrderConfirmed(leftValue.id, leftValue.productId, leftValue.customerId, true)
                            } else {
                                logger.warn("No customer found.")
                                null
                            }
                        })
                .filter { _, orderConfirmed -> orderConfirmed != null } // TODO is this needed
                .peek { key, orderConfirmed -> logger.info("Joined with customer {} {}", key, orderConfirmed) }
                .to(props.orderProcessedTopic, Produced.with(keySerde, orderConfirmedSerde))
        // OrderRejected Flow
        split[1]
                .mapValues { value -> value as OrderRejected }
                .selectKey { _, value -> value.id }
                .peek { key, _ -> logger.info("Order Rejected for {}", key) }
                .to(props.orderRejectedTopic, Produced.with(keySerde, orderRejectedSerde))

        // pass to override for optimization
        return streamsBuilder.build(streamConfig)
    }
}


