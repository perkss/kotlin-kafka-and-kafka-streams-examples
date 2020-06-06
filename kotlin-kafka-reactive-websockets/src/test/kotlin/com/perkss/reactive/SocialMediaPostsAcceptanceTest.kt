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
        val output: ReplayProcessor<Any> = ReplayProcessor.create(count)

        val client = ReactorNettyWebSocketClient()

        client.execute(
                getUrl("/event-emitter"))
        { session ->
            session.send(
                    input.map(session::textMessage))
                    .thenMany(session.receive().take(4L).map(WebSocketMessage::getPayloadAsText))
                    .subscribeWith(output)
                    .then()
        }.block(Duration.ofSeconds(10L))

        assertEquals(input.collectList().block(Duration.ofMillis(5000)), output.collectList().block(Duration.ofMillis(5000)));
    }

    protected fun getUrl(path: String): URI {
        return URI("ws://localhost:" + this.port.toString() + path)
    }

}