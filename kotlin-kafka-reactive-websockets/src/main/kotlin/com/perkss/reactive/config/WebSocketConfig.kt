package com.perkss.reactive.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.server.WebSocketService
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy

@Configuration
class WebSocketConfig {

    @Bean
    fun webSocketHandlerMapping(webSocketHandler: WebSocketHandler): HandlerMapping {
        val map = mapOf("/social-media-posts" to webSocketHandler)
        return SimpleUrlHandlerMapping(map, -1)
    }

    @Bean
    fun handlerAdapter() =  WebSocketHandlerAdapter()

    @Bean
    fun webSocketService(): WebSocketService? {
        return HandshakeWebSocketService(ReactorNettyRequestUpgradeStrategy())
    }

}