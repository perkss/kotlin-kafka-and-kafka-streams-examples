package com.perkss.kafka.reactive

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*

class KafkaReactiveConsumer(
    bootstrapServers: String,
    topic: String,
    sslEnabled: Boolean,
    saslEnabled: Boolean
) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveConsumer::class.java)
    }

    private val receiver: Flux<ConsumerRecord<String, String>>

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        if (sslEnabled) {
            consumerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
            // TODO properly property this could DockerFile it and mount the secrets at runtime
            consumerProps["ssl.truststore.location"] =
                "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.consumer.truststore.jks"
            consumerProps["ssl.truststore.password"] = "my-test-password"
            consumerProps[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = " "
        }

        if (saslEnabled) {
            consumerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SASL_SSL"
            consumerProps["sasl.jaas.config"] = "org.apache.kafka.common.security.plain.PlainLoginModule required \n" +
                    "  username=\"client\" \n" +
                    "  password=\"client-secret\";"
            consumerProps["sasl.mechanism"] = "PLAIN"
        }

        val consumerOptions =
            ReceiverOptions.create<String, String>(consumerProps).subscription(Collections.singleton(topic))

        receiver = KafkaReceiver.create<String, String>(consumerOptions)
            .receiveAutoAck()
            .concatMap { it }
    }

    fun consume() = receiver

}