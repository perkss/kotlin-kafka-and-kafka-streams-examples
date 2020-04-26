package com.perkss.kafka.reactive

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "perkss.kafka.example")
class AppProperties {
    lateinit var bootstrapServers: String
    lateinit var tableTopic: String
    lateinit var inputTopic: String
    lateinit var outputTopic: String
    lateinit var applicationId: String
    lateinit var stateDir: String
}