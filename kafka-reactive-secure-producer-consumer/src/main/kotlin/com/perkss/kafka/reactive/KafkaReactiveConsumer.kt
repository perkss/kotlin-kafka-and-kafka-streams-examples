package com.perkss.kafka.reactive

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.util.*

class KafkaReactiveConsumer(bootstrapServers: String,
                            topic: String,
                            sslEnabled: Boolean = true) {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveConsumer::class.java)
    }

    private val receiver: Flux<ReceiverRecord<String, String>>

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        // consumerProps["security.protocol"] = "PLAINTEXT"

        if (sslEnabled) {
            consumerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
            consumerProps["ssl.truststore.location"] = "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.consumer.truststore.jks"
            consumerProps["ssl.truststore.password"] = "my-test-password"
            consumerProps["ssl.keystore.location"] = "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.consumer.keystore.jks"
            consumerProps["ssl.keystore.password"] = "my-test-password"
            consumerProps["ssl.key.password"] = "my-test-password"
            consumerProps[SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG] = " "
        }


        val consumerOptions = ReceiverOptions.create<String, String>(consumerProps).subscription(Collections.singleton(topic))

        receiver = KafkaReceiver.create<String, String>(consumerOptions)
                .receive()
    }

    fun consume() = receiver

}