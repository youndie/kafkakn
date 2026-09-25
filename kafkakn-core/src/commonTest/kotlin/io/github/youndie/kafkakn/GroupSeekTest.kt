package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-51](../../../../../../../docs/backlog/B-51-seek-under-a-subscription.md): seek in a group, within the
 * partitions the group gave, and from the listener as they arrive.
 *
 * On the consumer fixture: one partition of twenty records from offset 0, so a lone member of a new group
 * is given that partition, and every offset below is one the broker holds.
 */
class GroupSeekTest {
    private val partition = TopicPartition(consumeTopic, 0)

    @Test
    fun a_member_seeks_a_partition_it_holds_and_reads_from_exactly_there() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                withMember { consumer ->
                    consumer.subscribe(listOf(consumeTopic))
                    val first = readUntil(consumer) { it.size >= CONSUME_COUNT }
                    assertEquals(0L, first.first().offset, "a new group starts at earliest")
                    consumer.seek(partition, SeekTo.Offset(SEEK_TO))
                    val position = consumer.position(partition)
                    val after = readUntil(consumer) { it.isNotEmpty() }
                    assertEquals(SEEK_TO, position, "the position right after the seek")
                    assertEquals(SEEK_TO, after.first().offset, "the first record after the seek")
                    recordObservation("group.seek.position", position.toString())
                    recordObservation("group.seek.first", after.first().offset.toString())
                }
            }
        }

    @Test
    fun a_seek_of_a_partition_this_member_does_not_hold_is_refused_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                withMember { consumer ->
                    consumer.subscribe(listOf(consumeTopic))
                    readUntil(consumer) { it.isNotEmpty() }
                    assertFailsWith<IllegalStateException> {
                        consumer.seek(TopicPartition(testTopic, 1), SeekTo.Offset(0))
                    }
                }
            }
        }

    @Test
    fun a_seek_made_on_assignment_takes_effect_before_the_first_record() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                withMember { consumer ->
                    val listener =
                        object : RebalanceListener {
                            override fun onAssigned(
                                partitions: List<TopicPartition>,
                                scope: RebalanceScope,
                            ) {
                                if (partition in partitions) scope.seek(partition, SeekTo.Offset(ON_ASSIGNMENT))
                            }
                        }
                    consumer.subscribe(listOf(consumeTopic), listener)
                    val read = readUntil(consumer) { it.isNotEmpty() }
                    assertEquals(ON_ASSIGNMENT, read.first().offset, "the first record the member was given")
                    recordObservation("group.seek.on.assignment", read.first().offset.toString())
                }
            }
        }

    private suspend fun withMember(use: suspend (KafkaConsumer) -> Unit) {
        val consumer =
            kafkaConsumer(
                ConsumerConfig(
                    "bootstrap.servers" to bootstrap,
                    "group.id" to "kafkakn-group-seek-$armName-${randomSuffix()}",
                    "auto.offset.reset" to "earliest",
                ),
            )
        try {
            use(consumer)
        } finally {
            consumer.close()
        }
    }

    private suspend fun readUntil(
        consumer: KafkaConsumer,
        enough: (List<ConsumerRecord>) -> Boolean,
    ): List<ConsumerRecord> {
        val read = mutableListOf<ConsumerRecord>()
        val deadline = TimeSource.Monotonic.markNow() + READ_FOR
        while (!enough(read) && deadline.hasNotPassedNow()) read += consumer.poll(POLL)
        check(enough(read)) { "read ${read.size} records in $READ_FOR" }
        return read
    }

    private companion object {
        const val SEEK_TO = 7L
        const val ON_ASSIGNMENT = 12L
        val POLL = 200.milliseconds
        val READ_FOR = 30.seconds
    }
}
