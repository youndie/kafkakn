package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-49](../../../../../../../docs/backlog/B-49-committed-and-position.md): read back where a consumer is,
 * and what its group committed.
 *
 * On the consumer fixture, whose shape is known: twenty records in one partition, the earliest at offset
 * 0 (retention is off). The item's trap is the position before any record is read, where `rdkafka.h`
 * says librdkafka has none. So most of these read the position *before* a poll, and every answer is a
 * number both arms must give. What the group committed is also read by `kafka-consumer-groups.sh`, in
 * `ci/b-49/run.sh`.
 */
class PositionTest {
    private val partition = TopicPartition(consumeTopic, 0)

    @Test
    fun the_position_is_the_next_offset_poll_will_return() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                withConsumer("kafkakn-position-$armName-${randomSuffix()}") { consumer ->
                    consumer.assign(listOf(partition))
                    val seen = mutableMapOf<String, Long>()

                    consumer.seek(partition, SeekTo.Beginning)
                    seen["after seek to the beginning"] = consumer.position(partition)
                    consumer.seek(partition, SeekTo.End)
                    seen["after seek to the end"] = consumer.position(partition)
                    consumer.seek(partition, SeekTo.Offset(SEEK_TO))
                    seen["after seek to an offset"] = consumer.position(partition)

                    consumer.seek(partition, SeekTo.Beginning)
                    val read = readAll(consumer)
                    seen["after reading everything"] = consumer.position(partition)
                    // And a seek after reading moves it back: the position is not just "what was read".
                    consumer.seek(partition, SeekTo.Offset(SEEK_TO))
                    seen["after reading, then seeking back"] = consumer.position(partition)

                    assertEquals(CONSUME_COUNT, read, "records read")
                    assertEquals(
                        mapOf(
                            "after seek to the beginning" to 0L,
                            "after seek to the end" to CONSUME_COUNT.toLong(),
                            "after seek to an offset" to SEEK_TO,
                            "after reading everything" to CONSUME_COUNT.toLong(),
                            "after reading, then seeking back" to SEEK_TO,
                        ),
                        seen,
                    )
                    seen.forEach { (moment, position) -> recordObservation("position.$moment", position.toString()) }
                }
            }
        }

    @Test
    fun before_any_record_a_group_member_is_where_its_group_committed() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val group = "kafkakn-position-committed-$armName-${randomSuffix()}"
                withConsumer(group) { first ->
                    first.assign(listOf(partition))
                    assertNull(first.committed(listOf(partition))[partition], "nothing committed yet")
                    first.commit(mapOf(partition to SEEK_TO))
                    val committed = first.committed(listOf(partition))
                    assertEquals(mapOf(partition to SEEK_TO), committed, "committed, read back")
                    recordArmFact("position.group", group)
                    recordObservation("position.committed", committed[partition].toString())
                }
                // A new consumer in the group, before its first poll: the committed offset.
                withConsumer(group) { second ->
                    second.assign(listOf(partition))
                    val position = second.position(partition)
                    assertEquals(SEEK_TO, position, "a fresh member's position is the group's commit")
                    recordObservation("position.fresh.member", position.toString())
                }
                // A new group with nothing committed, before its first poll: auto.offset.reset=earliest.
                withConsumer("kafkakn-position-new-$armName-${randomSuffix()}") { fresh ->
                    fresh.assign(listOf(partition))
                    val position = fresh.position(partition)
                    assertEquals(0L, position, "a new group's position is where auto.offset.reset says")
                    recordObservation("position.fresh.group", position.toString())
                }
            }
        }

    @Test
    fun the_position_of_a_partition_not_assigned_is_refused_on_both_arms() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                withConsumer("kafkakn-position-unassigned-$armName-${randomSuffix()}") { consumer ->
                    consumer.assign(listOf(partition))
                    assertFailsWith<IllegalStateException> { consumer.position(TopicPartition(testTopic, 1)) }
                }
            }
        }

    private suspend fun <T> withConsumer(
        group: String,
        use: suspend (KafkaConsumer) -> T,
    ): T {
        val consumer =
            kafkaConsumer(
                ConsumerConfig(
                    "bootstrap.servers" to bootstrap,
                    "group.id" to group,
                    "auto.offset.reset" to "earliest",
                ),
            )
        try {
            return use(consumer)
        } finally {
            consumer.close()
        }
    }

    private suspend fun readAll(consumer: KafkaConsumer): Int {
        var read = 0
        val deadline = TimeSource.Monotonic.markNow() + READ_FOR
        while (read < CONSUME_COUNT && deadline.hasNotPassedNow()) read += consumer.poll(POLL).size
        return read
    }

    private companion object {
        const val SEEK_TO = 7L
        val POLL = 500.milliseconds
        val READ_FOR = 30.seconds
    }
}
