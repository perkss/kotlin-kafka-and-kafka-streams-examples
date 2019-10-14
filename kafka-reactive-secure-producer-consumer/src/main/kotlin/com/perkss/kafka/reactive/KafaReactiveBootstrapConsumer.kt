package com.perkss.kafka.reactive

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration
import java.util.*

class KafaReactiveBootstrapConsumer(bootstrapServers: String,
                                    topic: String,
                                    sslEnabled: Boolean,
                                    saslEnabled: Boolean) {

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "sample-group"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        if (sslEnabled) {
            consumerProps[CommonClientConfigs.SECURITY_PROTOCOL_CONFIG] = "SSL"
            // TODO properly property this could DockerFile it and mount the secrets at runtime
            consumerProps["ssl.truststore.location"] = "/Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets/kafka.consumer.truststore.jks"
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

        val consumerOptions = ReceiverOptions.create<String, String>(consumerProps).subscription(Collections.singleton(topic))

        val kafkaReceiver = KafkaReceiver.create<String, String>(consumerOptions)
//                .doOnConsumer {  }
//                .receiveAutoAck()
//                .concatMap { it }

        val topicPartitions = kafkaReceiver.doOnConsumer {
            it.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
        }.block()

        kafkaReceiver.doOnConsumer { it.assign(topicPartitions) }.block()
        kafkaReceiver.doOnConsumer { it.seekToBeginning(it.assignment()) }.block()

        val endOffsets = kafkaReceiver.doOnConsumer { it.endOffsets(it.assignment()) }.block()
        fun pendingMessages() = endOffsets.any { endOffsets -> kafkaReceiver.doOnConsumer { it.position(endOffsets.key) < endOffsets.value }.block() }

        var records: ConsumerRecords<String, String>?

        do {
            records = kafkaReceiver.doOnConsumer { it.poll(Duration.ofMillis(1000)) }.block()
            // TODO send on onResult(records)
        } while (pendingMessages())

        // TODO then process on the records.


    }


}