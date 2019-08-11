package com.perkss.kafka.reactive

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import reactor.kafka.sender.SenderRecord
import reactor.core.publisher.Flux

@SpringBootApplication
class KafkaReactiveApp : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveApp::class.java)
    }

    override fun run(vararg args: String) {

        logger.info("Running Kafka Reactive App")

        val bootstrapServers = "localhost:9092"
        val topic = "test-topic"

        val outboundFlux = Flux.range(1, 10)
                .map { i -> SenderRecord.create(ProducerRecord( topic, i, "Message_" + i!!), i) }

        // val producer = KafkaReactiveProducer(bootstrapServers)

        //  producer.send(outboundFlux)

        val consumer = KafkaReactiveConsumer(bootstrapServers, topic)

        consumer.consume()
    }

}

fun main(args : Array<String>) {
    runApplication<KafkaReactiveApp>(*args)
}


