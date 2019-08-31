package com.perkss.kafka.reactive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import java.util.*

class KafkaReactiveConsumer(bootstrapServers: String,
                            topic: String,
                            consumerGroupId: String,
                            autoOffsetReset: String = "earliest") {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveConsumer::class.java)
    }

    private val receiver: Flux<ReceiverRecord<String, String>>

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset
        val consumerOptions = ReceiverOptions.create<String, String>(consumerProps).subscription(Collections.singleton(topic))

        receiver = KafkaReceiver.create<String, String>(consumerOptions)
                .receive()
                .map {
                    logger.info("Received message: $it")
                    it.receiverOffset().acknowledge()
                    it.receiverOffset().commit()
                    it
                }
    }

    fun consume() = receiver

}