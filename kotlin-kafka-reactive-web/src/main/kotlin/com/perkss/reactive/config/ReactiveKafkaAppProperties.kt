package com.perkss.reactive.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "perkss.kafka.example")
class ReactiveKafkaAppProperties {
    lateinit var bootstrapServers: String
    lateinit var inputTopic: String
    lateinit var consumerGroupId: String
}