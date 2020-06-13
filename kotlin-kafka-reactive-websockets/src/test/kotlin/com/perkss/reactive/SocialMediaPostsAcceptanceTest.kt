package com.perkss.reactive

import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import junit.framework.Assert.assertEquals
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.context.annotation.Bean
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import reactor.core.publisher.Flux
import reactor.core.publisher.ReplayProcessor
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverRecord
import java.net.URI
import java.time.Duration


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SocialMediaPostsAcceptanceTest {

    @LocalServerPort
    private val port: String? = null

    @Test
    fun websocketPostsFromKafka() {
        val count = 1
        val input: Flux<String> = Flux.just( "msg" )
        val output: ReplayProcessor<Any> = ReplayProcessor.create(count)

        val input2: Flux<String> = Flux.just( "msg" )
        val output2: ReplayProcessor<Any> = ReplayProcessor.create(count)

        val client = ReactorNettyWebSocketClient()
        val client2 = ReactorNettyWebSocketClient()

        client.execute(
                getUrl("/social-media-posts"))
        { session ->
            // Send frame then subscribe.
            session.send(
                    input.map(session::textMessage))
                    .thenMany(session.receive().take(1L).map(WebSocketMessage::getPayloadAsText))
                    .subscribeWith(output)
                    .then()
        }.block(Duration.ofSeconds(10L))

        client2.execute(
                getUrl("/social-media-posts"))
        { session ->
            session.send(
                    input2.map(session::textMessage))
                    .thenMany(session.receive().take(1L).map(WebSocketMessage::getPayloadAsText))
                    .subscribeWith(output2)
                    .then()
        }.block(Duration.ofSeconds(10L))

        assertEquals(listOf("First websocket post"), output.collectList().block(Duration.ofMillis(5000)))
        assertEquals(listOf("First websocket post"), output2.collectList().block(Duration.ofMillis(5000)))
    }

    protected fun getUrl(path: String): URI {
        return URI("ws://localhost:" + this.port.toString() + path)
    }

    @TestConfiguration
    class Config {

        @Bean
        fun kafkaMessages(): KafkaReceiver<String, String> =
                mock {
                    on { receive() } doAnswer {
                        Flux.just(
                                ReceiverRecord(
                                        ConsumerRecord("social-media", 0, 0, "Alice", "First websocket post"),
                                        mock()))
                    }

                }
    }
}