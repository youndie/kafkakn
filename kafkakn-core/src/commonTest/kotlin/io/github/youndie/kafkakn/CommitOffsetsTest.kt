package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-48](../../../../../../../docs/backlog/B-48-commit-explicit-offsets.md): commit named offsets, not
 * only everything `poll` returned.
 *
 * Read on the consumer fixture: twenty records in one partition, written by the distribution's own
 * client. One consumer reads the whole batch and commits offset [COMMITTED], the middle of it. A second
 * consumer in the same group then starts where the group says. The commit itself is read by
 * `kafka-consumer-groups.sh` in `ci/b-48/run.sh`, never by the consumer that made it.
 */
class CommitOffsetsTest {
    private val partition = TopicPartition(consumeTopic, 0)

    @Test
    fun a_named_offset_is_what_the_group_resumes_from() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val group = "kafkakn-commit-$armName-${randomSuffix()}"
                withConsumer(group) { first ->
                    first.assign(listOf(partition))
                    first.seek(partition, SeekTo.Beginning)
                    val read = readAtLeast(first, CONSUME_COUNT)
                    assertEquals(CONSUME_COUNT, read.size, "the first consumer read the whole fixture")
                    first.commit(mapOf(partition to COMMITTED))
                }
                val resumed =
                    withConsumer(group) { second ->
                        second.assign(listOf(partition))
                        readAtLeast(second, 1)
                    }
                assertEquals(COMMITTED, resumed.first().offset, "the group resumed from the offset committed")
                assertEquals(
                    (COMMITTED until CONSUME_COUNT).toList(),
                    resumed.map { it.offset },
                    "the records after it, and none before",
                )
                recordArmFact("commit.group", group)
                recordObservation("commit.resumed.from", resumed.first().offset.toString())
            }
        }

    @Test
    fun a_partition_this_consumer_does_not_hold_is_measured_not_assumed() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val group = "kafkakn-commit-unheld-$armName-${randomSuffix()}"
                val outcome =
                    withConsumer(group) { consumer ->
                        consumer.assign(listOf(partition))
                        readAtLeast(consumer, 1)
                        try {
                            consumer.commit(mapOf(TopicPartition(testTopic, 1) to 0L))
                            "accepted"
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (refused: Exception) {
                            "refused"
                        }
                    }
                recordArmFact("commit.unheld.group", group)
                // What only the client knows, compared across the arms by compare-arms.sh: the contract
                // states whichever answer this is, or the difference.
                recordObservation("commit.unheld", outcome)
            }
        }

    @Test
    fun a_negative_offset_is_refused_before_it_reaches_a_client() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                withConsumer("kafkakn-commit-negative-$armName-${randomSuffix()}") { consumer ->
                    consumer.assign(listOf(partition))
                    assertFails { consumer.commit(mapOf(partition to -1L)) }
                }
            }
        }

    @Test
    fun committing_nothing_commits_nothing() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                withConsumer("kafkakn-commit-empty-$armName-${randomSuffix()}") { consumer ->
                    consumer.assign(listOf(partition))
                    consumer.commit(emptyMap())
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

    private suspend fun readAtLeast(
        consumer: KafkaConsumer,
        count: Int,
    ): List<ConsumerRecord> {
        val read = mutableListOf<ConsumerRecord>()
        val deadline = TimeSource.Monotonic.markNow() + READ_FOR
        while (read.size < count && deadline.hasNotPassedNow()) {
            read += consumer.poll(POLL)
        }
        // Past the first batch: whatever else is available, so "none before" and "all after" are both
        // about the whole partition, not about a batch boundary.
        while (true) {
            val more = consumer.poll(POLL)
            if (more.isEmpty()) break
            read += more
        }
        assertTrue(read.size >= count, "read ${read.size} records, wanted at least $count")
        return read
    }

    private companion object {
        const val COMMITTED = 7L
        val POLL = 500.milliseconds
        val READ_FOR = 30.seconds
    }
}
