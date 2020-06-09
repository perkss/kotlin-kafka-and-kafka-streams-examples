package com.perkss.reactive

import junit.framework.Assert.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.web.server.LocalServerPort
import org.springframework.web.reactive.socket.WebSocketMessage
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import reactor.core.publisher.Flux
import reactor.core.publisher.ReplayProcessor
import java.net.URI
import java.time.Duration


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SocialMediaPostsAcceptanceTest {

    @LocalServerPort
    private val port: String? = null

    @Test
    fun websocketPosts() {
        val count = 4
        val input: Flux<String> = Flux.range(1, count).map { index -> "msg-$index" }
        val expectedOutput = input.map { "Processed $it" }
        val output: ReplayProcessor<Any> = ReplayProcessor.create(count)

        val input2: Flux<String> = Flux.range(5, count).map { index -> "msg-$index" }
        val expectedOutput2 = input2.map { "Processed $it" }
        val output2: ReplayProcessor<Any> = ReplayProcessor.create(count)

        val client = ReactorNettyWebSocketClient()
        val client2 = ReactorNettyWebSocketClient()

        client.execute(
                getUrl("/social-media-posts"))
        { session ->
            // Send frame then subscribe.
            session.send(
                    input.map(session::textMessage))
                    .thenMany(session.receive().take(4L).map(WebSocketMessage::getPayloadAsText))
                    .subscribeWith(output)
                    .then()
        }.block(Duration.ofSeconds(10L))

        client2.execute(
                getUrl("/social-media-posts"))
        { session ->
            session.send(
                    input2.map(session::textMessage))
                    .thenMany(session.receive().take(4L).map(WebSocketMessage::getPayloadAsText))
                    .subscribeWith(output2)
                    .then()
        }.block(Duration.ofSeconds(10L))

        assertEquals(expectedOutput.collectList().block(Duration.ofMillis(5000)), output.collectList().block(Duration.ofMillis(5000)))
        assertEquals(expectedOutput2.collectList().block(Duration.ofMillis(5000)), output2.collectList().block(Duration.ofMillis(5000)));
    }

    protected fun getUrl(path: String): URI {
        return URI("ws://localhost:" + this.port.toString() + path)
    }

}