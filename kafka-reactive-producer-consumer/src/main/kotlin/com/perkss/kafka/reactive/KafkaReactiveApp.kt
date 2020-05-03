package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.config.ReactiveKafkaAppProperties
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import reactor.core.publisher.Mono
import reactor.kafka.sender.SenderRecord

@SpringBootApplication
class KafkaReactiveApp(private var consumer: KafkaReactiveConsumer<String, String>,
                       private var producer: KafkaReactiveProducer<String, String>,
                       private var reactiveKafkaAppProperties: ReactiveKafkaAppProperties) : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaReactiveApp::class.java)
    }

    override fun run(vararg args: String) {

        logger.info("Running Kafka Reactive App: Uppercase Topology")

        val outputTopic = reactiveKafkaAppProperties.outputTopic

        consumer.consume()
                .map {
                    val producerRecord = ProducerRecord(outputTopic, it.key(), it.value().toUpperCase())
                    logger.info("Building uppercase message. Key: ${it.key()} Message: ${it.value().toUpperCase()}")
                    SenderRecord.create(producerRecord, it.key())
                }
                .map { producer.send(Mono.just(it)).subscribe() }
                .doOnError { logger.error("An error has occurred $it") }
                .subscribe {
                    logger.info("Subscribing to Consumer and Producer")
                }
    }

}

fun main(args: Array<String>) {
    runApplication<KafkaReactiveApp>(*args)
}


