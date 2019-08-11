package com.perkss.kafka.reactive

/**
 * A function to say hello to given user.
 *
 * @param name user name, optional
 * @return greeting string to given user
 *
 */
fun sayHi(name: String = "World"): String = "Hello $name!"