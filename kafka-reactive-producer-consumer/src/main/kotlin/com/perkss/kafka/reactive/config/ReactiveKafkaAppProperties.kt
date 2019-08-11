package com.perkss.kafka.reactive.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "perkss.kafka.example")
class ReactiveKafkaAppProperties {
    lateinit var bootstrapServers: String
    lateinit var inputTopic: String
}