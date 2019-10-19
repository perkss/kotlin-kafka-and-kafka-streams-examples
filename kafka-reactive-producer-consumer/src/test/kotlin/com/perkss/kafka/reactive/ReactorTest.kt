package com.perkss.kafka.reactive

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux

class ReactorTest {

    @Test
    fun `Testing Reactor branch publisher`() {
        val flux = Flux.just("MyData1", "MyData2", "MyData3").publish().refCount(2)

        flux.doOnNext { println("Subscribing one$it") }.subscribe()

        flux.doOnNext { println("Subscribing Two$it") }.subscribe()
    }


}