package com.perkss.kafka.reactive

import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStreamsApp(
    private val orderProcessingApp: KafkaStreams,
    private val bootstrapSemantics: KafkaStreams
) : CommandLineRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaStreamsApp::class.java)
    }

    override fun run(vararg args: String) {
        logger.info("Running Kotlin Kakfa Streams")
        // orderProcessingApp.start()
        bootstrapSemantics.start()
    }
}

fun main(args: Array<String>) {
    runApplication<KafkaStreamsApp>(*args)
}


