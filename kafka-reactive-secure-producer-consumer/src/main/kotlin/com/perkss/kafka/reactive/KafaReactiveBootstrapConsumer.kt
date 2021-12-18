package com.perkss.kafka.reactive

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.StringDeserializer
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*

class KafaReactiveBootstrapConsumer(
    bootstrapServers: String,
    private val topic: String,
    sslEnabled: Boolean,
    saslEnabled: Boolean
) {
    private var kafkaReceiver: KafkaReceiver<String, String>

    init {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "bootstrap-group"
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


        // TODO then process on the records.
        val consumerOptions = ReceiverOptions.create<String, String>(consumerProps)
            .commitBatchSize(1)
            .addAssignListener { partitions -> partitions.forEach { p -> p.seekToBeginning() } }
            .subscription(Collections.singleton(topic))
        kafkaReceiver = KafkaReceiver.create<String, String>(consumerOptions)


    }

    fun bootstrap() {

        println("Bootstrapping")


//                .doOnConsumer {  }
//                .receiveAutoAck()
//                .concatMap { it }


//        kafkaReceiver.doOnConsumer {
//            it.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }
//        }
//                .flatMap { kafkaReceiver.doOnConsumer { consumer -> consumer.assign(it) } }

        class Progress {
            var endOffsets = emptyMap<TopicPartition, Long>()

            fun setEndOffset(endOffsets: MutableMap<TopicPartition, Long>) {
                println("Setting end offset to $endOffsets")
                if (endOffsets.isEmpty()) {
                    this.endOffsets = endOffsets
                }
            }

            fun isPending(): Mono<Boolean> {
                println("Checking for pending")
                return Mono.just(endOffsets)
                    .flatMapIterable { it.entries }
                    .map { (key, value) -> kafkaReceiver.doOnConsumer { it.position(key) < value } }
                    .collectList()
                    .map { it.any() }
            }
        }

        var progress = Progress()


        kafkaReceiver
            .receiveAutoAck()
            .concatMap { r -> r }
            .doOnNext { r ->
                println("Consuming ${r.value()}")
            }
            .map { kafkaReceiver.doOnConsumer { consumer -> consumer.endOffsets(consumer.assignment()) } }
            .flatMap { it }
            .map { progress.setEndOffset(it) }
            .map { progress.isPending() }
            .flatMap {
                it
            }
            .takeWhile {
                println("Pending is $it")
                it
            }
            .subscribe()

//                .and {
//                    kafkaReceiver
//                            .receiveAutoAck()
//                            .concatMap { r -> r }
//                            .doOnNext { r ->
//                                println("Consuming ${r.value()}")
//                            }}
//                        .map { endOffsets.map { endOffset -> kafkaReceiver.doOnConsumer { it.position(endOffset.key) }} }
//                        .flatMapIterable { it }
//                        .flatMap { it }
//                        .map {  }
//                        //.doOnNext { println("Pending messages is $it") }
//                        .takeWhile {
//                            println("Pending messages is $it")
//                            it == true
//                        } }
//                .map { endOffset ->
//                    val data =      kafkaReceiver.doOnConsumer { it.position(endOffset.key) < endOffset.value }
//                    println("Current offset is ${kafkaReceiver.doOnConsumer { it.position(endOffset.key) }} offset value ${endOffset.value}")
//                    data
//                }
//                .collectList()
//                .map { it.any() }


//        kafkaReceiver.doOnConsumer { it.assign(topicPartitions) }.block()
//        kafkaReceiver.doOnConsumer { it.seekToBeginning(it.assignment()) }.block()

//        val endOffsets = kafkaReceiver.doOnConsumer { it.endOffsets(it.assignment()) }
//        fun pendingMessages() = endOffsets.any { endOffsets -> kafkaReceiver.doOnConsumer { it.position(endOffsets.key) < endOffsets.value } }
//
//        var records: ConsumerRecords<String, String>?
//
//        do {
//            records = kafkaReceiver.doOnConsumer { it.poll(Duration.ofMillis(1000)) }.block()
//            // TODO send on onResult(records)
//        } while (pendingMessages())


    }


}