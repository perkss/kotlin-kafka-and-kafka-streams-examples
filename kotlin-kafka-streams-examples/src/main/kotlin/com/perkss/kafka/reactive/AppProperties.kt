package com.perkss.kafka.reactive

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "perkss.kafka.example")
class AppProperties {
    lateinit var bootstrapServers: String
    lateinit var schemaRegistry: String
    lateinit var stockInventory: String
    lateinit var customerInformation: String
    lateinit var orderRequest: String
    lateinit var orderProcessedTopic: String
    lateinit var applicationId: String
    lateinit var stateDir: String
}