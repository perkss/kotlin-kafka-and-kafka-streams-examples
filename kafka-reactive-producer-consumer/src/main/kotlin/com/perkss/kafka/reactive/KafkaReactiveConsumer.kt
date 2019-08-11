package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.util.*

class KafkaReactiveConsumer(bootstrapServers: String,
                            topic: String) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveConsumer::class.java)
    }

    private val inboundFlux: Flux<ReceiverRecord<Int, String>>

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        val consumerOptions = ReceiverOptions.create<Int, String>(consumerProps).subscription(Collections.singleton(topic))

        inboundFlux = KafkaReceiver.create<Int, String>(consumerOptions)
                .receive()
    }

    fun consume() =
            inboundFlux.subscribe {
                logger.info("Received message: {}", it)
                it.receiverOffset().acknowledge() // TODO is both required.
                it.receiverOffset().commit() }

}