package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Clock

/**
 * [B-28](../../../../../../../docs/backlog/B-28-a-record-carries-its-timestamp.md): a record carries its
 * timestamp, and [RecordMetadata] says which time the broker kept.
 *
 * **Half of the assertion is in `ci/b-28/run.sh`**: [RecordMetadata.timestamp] is the client relaying
 * the broker's answer, and the party that says what is stored is `kafka-console-consumer` printing each
 * record's timestamp and its type. The stamps below are how the script finds these records.
 */
class TimestampTest {
    private fun producer() = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap))

    @Test
    fun a_record_that_names_its_time_is_stored_with_it() =
        runTest {
            withContext(Dispatchers.Default) {
                val stamp = "time-$armName-${randomSuffix()}"
                val producer = producer()
                val where =
                    try {
                        producer.send(ProducerRecord(testTopic, stamp.encodeToByteArray(), timestamp = CHOSEN))
                    } finally {
                        producer.close()
                    }
                assertEquals(CHOSEN, where.timestamp, "the metadata does not carry the time the record named")
                recordArmFact("timestamp.create.stamp", stamp)
            }
        }

    @Test
    fun a_record_that_names_none_gets_the_client_clock_at_send() =
        runTest {
            withContext(Dispatchers.Default) {
                val producer = producer()
                val before = hostNow()
                val where =
                    try {
                        producer.send(ProducerRecord(testTopic, "untimed".encodeToByteArray()))
                    } finally {
                        producer.close()
                    }
                val after = hostNow()
                assertTrue(
                    where.timestamp in before..after,
                    "an untimed record should carry the moment it was sent, $before..$after: ${where.timestamp}",
                )
            }
        }

    @Test
    fun on_a_log_append_topic_the_broker_clock_replaces_the_one_the_record_named() =
        runTest {
            withContext(Dispatchers.Default) {
                val stamp = "logappend-$armName-${randomSuffix()}"
                val producer = producer()
                val before = hostNow()
                val where =
                    try {
                        producer.send(ProducerRecord(logAppendTopic, stamp.encodeToByteArray(), timestamp = CHOSEN))
                    } finally {
                        producer.close()
                    }
                val after = hostNow()
                // The broker and this test share a machine, so its clock and ours agree to within the
                // time the call took - which is the bound, and a 2020 timestamp is far outside it.
                assertNotEquals(CHOSEN, where.timestamp, "a LogAppendTime topic kept the record's own time")
                assertTrue(
                    where.timestamp in before..after,
                    "expected the broker's clock, $before..$after; the metadata says ${where.timestamp}",
                )
                recordArmFact("timestamp.logappend.stamp", stamp)
            }
        }

    @Test
    fun a_negative_timestamp_is_refused_where_the_record_is_made() {
        assertFails { ProducerRecord(testTopic, ByteArray(0), timestamp = -1) }
    }

    @Suppress(
        "ktlint:kapkan:wall-clock",
        "the bound for a timestamp the broker or the client took from the same machine's clock",
    )
    private fun hostNow(): Long = Clock.System.now().toEpochMilliseconds()

    private companion object {
        /** 2020-09-13T12:26:40Z: far enough from now that no clock could produce it by accident. */
        const val CHOSEN = 1_600_000_000_000L
    }
}
