package com.perkss.kafka.reactive

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Greeting test case.
 */
class GreetingTest {

    /**
     * Given the user name is "Kotlin"
     * When we call "sayHai" with user name.
     * Then it should return "Hello Kotlin!"
     */
    @Test
    fun sayHiWithParameter() {
        assertEquals("Hello Kotlin!", sayHi("Kotlin"))
    }

    /**
     * Given the user name is not specified
     * When we call "sayHai" without any parameter.
     * Then it should return "Hello World!"
     */
    @Test
    fun sayHiWithoutParameter() {
        assertEquals("Hello World!", sayHi())
    }
}