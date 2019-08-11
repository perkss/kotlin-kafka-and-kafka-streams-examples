package com.perkss.kafka.reactive

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.util.*

class KafkaReactiveProducer(bootstrapServers: String) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveProducer::class.java)
    }

    private val sender: KafkaSender<Int, String>

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        val senderOptions = SenderOptions.create<Int, String>(producerProps).maxInFlight(1024)

        sender = KafkaSender.create(senderOptions)
    }

    fun send(outboundFlux: Publisher<SenderRecord<Int, String, Int>>) {
        sender.send(outboundFlux)
                .doOnError { e -> logger.error("Send failed", e) }
                .doOnNext { r -> logger.info("Message #%d send response: %s\n", r.correlationMetadata(), r.recordMetadata()) }
                .subscribe()
    }
}