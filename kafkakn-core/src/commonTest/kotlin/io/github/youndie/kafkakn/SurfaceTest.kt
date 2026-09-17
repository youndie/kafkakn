package io.github.youndie.kafkakn

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * The surface exists, is reachable on both arms, and does nothing yet.
 *
 * Red for the right reason is the point of this file: the first real producer test has to fail
 * because the producer is wrong, not because the suite could not construct one.
 */
class SurfaceTest {

    @Test
    fun a_producer_can_be_constructed_on_this_arm() {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to "127.0.0.1:9092"))
        assertEquals(UnimplementedProducer, producer)
    }

    @Test
    fun send_is_not_implemented_yet_on_this_arm() = runTest {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to "127.0.0.1:9092"))
        assertFailsWith<NotImplementedError> {
            producer.send(ProducerRecord(topic = "t", value = byteArrayOf(1, 2, 3)))
        }
    }

    @Test
    fun configuration_keeps_kafka_s_own_key_names() {
        val config = ProducerConfig("bootstrap.servers" to "h:9092", "acks" to "all")
        assertEquals("h:9092", config["bootstrap.servers"])
        assertEquals("all", config["acks"])
        assertNull(config["bootstrapServers"])
    }

    @Test
    fun a_record_carries_bytes_and_an_optional_key() {
        val record = ProducerRecord(topic = "t", value = byteArrayOf(0, -1, 2))
        assertNull(record.key)
        assertEquals(3, record.value.size)
    }
}
