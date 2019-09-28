package com.perkss.kafka.reactive

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringSerializer
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.util.*

class KafkaReactiveProducer(bootstrapServers: String,
                            sslEnabled: Boolean,
                            saslEnabled: Boolean) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveProducer::class.java)
    }

    private val sender: KafkaSender<String, String>

    init {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        if (sslEnabled) {
            producerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
            producerProps[SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG] = "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.producer.truststore.jks"
            producerProps[SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG] = "my-test-password"
            producerProps[SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG] = "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.producer.keystore.jks"
            producerProps[SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG] = "my-test-password"
            producerProps[SslConfigs.SSL_KEY_PASSWORD_CONFIG] = "my-test-password"
            producerProps[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = " "
        }

        if (saslEnabled) {
            producerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
            producerProps["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required \n" +
                    "  username=\"client\" \n" +
                    "  password=\"client-secret\";"
            producerProps["sasl.mechanism"] = "PLAIN"
        }

        val senderOptions = SenderOptions.create<String, String>(producerProps).maxInFlight(1024)

        sender = KafkaSender.create(senderOptions)
    }

    fun send(outboundFlux: Publisher<SenderRecord<String, String, String>>) {
        sender.send(outboundFlux)
                .doOnError { e -> logger.error("Send failed", e) }
                .doOnNext { r -> logger.info("Message Key {} send response to TLS connected topic: {}", r.correlationMetadata(), r.recordMetadata().topic()) }
                .subscribe()
    }
}