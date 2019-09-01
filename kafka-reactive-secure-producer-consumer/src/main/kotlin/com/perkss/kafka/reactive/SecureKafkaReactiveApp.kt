package com.perkss.kafka.reactive

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderRecord

@SpringBootApplication
class SecureKafkaReactiveApp : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(SecureKafkaReactiveApp::class.java)
    }

    @Autowired
    private lateinit var consumer: KafkaReactiveConsumer

    @Autowired
    private lateinit var producer: KafkaReactiveProducer

    override fun run(vararg args: String) {

        logger.info("Running Secured Kafka Reactive App: Uppercase Topology")

        val outputTopic = "uppercase-topic"

        consumer.consume()
                .map {
                    val producerRecord = ProducerRecord(outputTopic, it.key(), it.value().toUpperCase())
                    SenderRecord.create(producerRecord, it.key())
                }
                .map { producer.send(Mono.just(it)) }
                .doOnError { logger.error("An error has occurred {}", it) }
                .subscribe {
                    logger.info("Subscribing to Consumer and Producer")
                }
    }

}

fun main(args: Array<String>) {
    runApplication<SecureKafkaReactiveApp>(*args)
}


