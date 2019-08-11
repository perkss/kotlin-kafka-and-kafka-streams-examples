package com.perkss.kafka.reactive.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "perkss.kafka.example")
class ReactiveKafkaAppProperties {
    var bootstrapServers: String = "localhost:9092"
    var inputTopic: String = "lowercase-topic"
}