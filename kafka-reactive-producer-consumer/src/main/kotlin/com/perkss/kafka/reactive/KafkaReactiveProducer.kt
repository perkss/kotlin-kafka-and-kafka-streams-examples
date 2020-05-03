package com.perkss.kafka.reactive

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.util.*

class KafkaReactiveProducer<K, V>(bootstrapServers: String) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveProducer::class.java)
    }

    private val sender: KafkaSender<K, V>

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        val senderOptions = SenderOptions.create<K, V>(producerProps).maxInFlight(1024)

        sender = KafkaSender.create(senderOptions)
    }

    fun send(outboundFlux: Mono<SenderRecord<K, V, String>>): Flux<SenderResult<String>> {
        return sender.send(outboundFlux)
                .doOnError { e -> logger.error("Send failed", e) }
                .doOnNext { r -> logger.info("Message Key ${r.correlationMetadata()} send response: ${r.recordMetadata().topic()}") }
    }
}