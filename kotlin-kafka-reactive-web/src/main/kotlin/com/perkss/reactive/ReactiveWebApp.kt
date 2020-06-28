package com.perkss.reactive

import com.perkss.reactive.config.ReactiveKafkaAppProperties
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReactiveWebApp(private val properties: ReactiveKafkaAppProperties) : CommandLineRunner {
    companion object {
        private val logger = LoggerFactory.getLogger(ReactiveWebApp::class.java)
    }

    override fun run(vararg args: String?) {
        logger.info("Bootstrap Services is {}, Topic is {}",
                properties.bootstrapServers, properties.inputTopic)
    }
}

fun main(args: Array<String>) {
    runApplication<ReactiveWebApp>(*args)
}