package com.perkss.kafka.reactive

import com.perkss.kafka.reactive.config.ReactiveKafkaAppProperties
import org.springframework.jmx.export.annotation.ManagedOperation
import org.springframework.jmx.export.annotation.ManagedResource
import org.springframework.stereotype.Component


@Component
@ManagedResource(objectName = "bean:name=Bootstrap", description = "Bootstraps from Kafka")
class BootstrapMbean(private val propertiesReactiveKafka: ReactiveKafkaAppProperties) {

    @ManagedOperation(description = "Bootstrap")
    fun bootstrap() {

        val consumer = KafaReactiveBootstrapConsumer(propertiesReactiveKafka.bootstrapServers,
                propertiesReactiveKafka.inputTopic, propertiesReactiveKafka.sslEnabled, propertiesReactiveKafka.saslEnabled)
        consumer.bootstrap()

    }

}