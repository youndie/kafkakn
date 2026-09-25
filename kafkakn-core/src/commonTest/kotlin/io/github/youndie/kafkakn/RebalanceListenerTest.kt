package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-50](../../../../../../../docs/backlog/B-50-a-rebalance-listener.md): the rebalance listener, as
 * consumer-contract §2a designs it.
 *
 * The group test is B-37's mixed group with one change that makes duplicates visible: **a member commits
 * only in its revocation callback**, never in its loop. So every record it processed and did not commit
 * on revocation is delivered again to whoever takes the partition. `ci/b-50/run.sh` starts one member
 * on each arm, counts duplicates as well as losses against the broker, and reads each listener's events.
 */
class RebalanceListenerTest {
    /** What one member's listener reported, in order: `+[0,1]` assigned, `-[0,1]` revoked, `![0]` lost. */
    private class Events : RebalanceListener {
        val log = mutableListOf<String>()
        val processed = mutableMapOf<TopicPartition, Long>()
        val committedOnRevoke = mutableListOf<String>()

        override fun onAssigned(partitions: List<TopicPartition>) {
            log += "+${render(partitions)}"
        }

        override fun onRevoked(
            partitions: List<TopicPartition>,
            scope: RebalanceScope,
        ) {
            log += "-${render(partitions)}"
            // Commit what was processed on the partitions about to leave, and nothing else: this is the
            // item's whole point. Everything processed and not committed here reaches the next owner again.
            val leaving = processed.filterKeys { it in partitions }
            scope.commit(leaving)
            committedOnRevoke += leaving.entries.map { "${it.key.partition}:${it.value}" }
        }

        override fun onLost(partitions: List<TopicPartition>) {
            log += "!${render(partitions)}"
        }

        private fun render(partitions: List<TopicPartition>) =
            partitions
                .map {
                    it.partition
                }.sorted()
                .joinToString(",", "[", "]")
    }

    /**
     * One member of a group with one member on each arm, asked for through the environment and only then,
     * as B-37's mixed member is. `KAFKAKN_REBALANCE_LEAVE_AFTER` makes it the member that leaves (cleanly:
     * `close`, which revokes) once it has processed that many records; otherwise it stays until it holds
     * every partition and has seen their end. Both rules are data, not time: a leaver on a fixed fifteen
     * seconds once held its partitions for too short a moment to process any, and its revocation then
     * had nothing to commit, which proves nothing about the listener.
     */
    @Test
    fun a_member_that_commits_only_on_revocation_hands_over_without_loss_or_duplicates() =
        runTest(timeout = 3.minutes) {
            val group = testEnv("KAFKAKN_REBALANCE_GROUP")
            if (group == null) {
                recordArmFact("rebalance.member", "not asked")
                return@runTest
            }
            val topic = testEnv("KAFKAKN_REBALANCE_TOPIC") ?: error("KAFKAKN_REBALANCE_TOPIC")
            val leave = testEnv("KAFKAKN_REBALANCE_LEAVE_AFTER")?.toInt()
            val partitions = testEnv("KAFKAKN_REBALANCE_PARTITIONS")?.toInt() ?: 1
            val end = testEnv("KAFKAKN_REBALANCE_END")?.toLong() ?: 0L
            val events = Events()
            val seen = mutableListOf<String>()
            withContext(Dispatchers.Default) {
                val consumer = consumer(group)
                try {
                    consumer.subscribe(listOf(topic), events)
                    val started = TimeSource.Monotonic.markNow()
                    while (true) {
                        if (leave != null && seen.size >= leave) break
                        if (leave == null && holdsAllAndSawTheEnd(consumer, partitions, end, seen)) break
                        check(started.elapsedNow() < STAY_AT_MOST) { "gave up after $STAY_AT_MOST: ${events.log}" }
                        for (record in consumer.poll(POLL)) {
                            seen += "${record.partition}:${record.offset}"
                            events.processed[TopicPartition(record.topic, record.partition)] = record.offset + 1
                        }
                    }
                } finally {
                    // Leaving is a revocation: the listener commits what this member processed, here.
                    consumer.close()
                }
            }
            recordArmFact("rebalance.member", if (leave != null) "left" else "stayed")
            recordArmFact("rebalance.seen", seen.joinToString(";"))
            recordArmFact("rebalance.events", events.log.joinToString(" "))
            recordArmFact("rebalance.committed", events.committedOnRevoke.joinToString(";"))
            assertTrue(events.log.any { it.startsWith("+") }, "this member was never assigned anything: ${events.log}")
            if (leave != null) {
                assertTrue(events.log.last().startsWith("-"), "the member that left did not revoke last: ${events.log}")
            }
        }

    /**
     * §2a: the consumer must not be called from inside a callback. It would wait for the `poll` that is
     * waiting for the callback; the contract says it throws instead, on both arms.
     */
    @Test
    fun calling_the_consumer_from_inside_a_callback_throws_instead_of_deadlocking() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                val outcome = mutableListOf<String>()
                val consumer = consumer("kafkakn-rebalance-reentry-$armName-${randomSuffix()}")
                val listener =
                    object : RebalanceListener {
                        override fun onAssigned(partitions: List<TopicPartition>) {
                            outcome +=
                                try {
                                    // Bounded, so an unguarded call fails by name instead of hanging the run:
                                    // it deadlocks by SUSPENDING (on the JVM's lane, on native's lock), so the
                                    // timeout can still fire. Without it, a run with the guard removed never
                                    // finished, measured twice.
                                    val answer =
                                        runBlocking { withTimeoutOrNull(REENTRY_WAIT) { consumer.assignment() } }
                                    if (answer == null) "deadlocked" else "answered"
                                } catch (cancelled: CancellationException) {
                                    throw cancelled
                                } catch (refused: IllegalStateException) {
                                    recordArmFact("rebalance.reentry.message", refused.message.orEmpty())
                                    "refused"
                                }
                        }
                    }
                try {
                    consumer.subscribe(listOf(consumeTopic), listener)
                    val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
                    while (outcome.isEmpty() && until.hasNotPassedNow()) consumer.poll(POLL)
                } finally {
                    consumer.close()
                }
                assertEquals(listOf("refused"), outcome, "a call from inside onAssigned")
                recordObservation("rebalance.reentry", outcome.single())
            }
        }

    /** §2a: a callback is not called with an empty list, on either arm. */
    @Test
    fun a_lone_member_is_assigned_its_partitions_once_and_revoked_them_on_close() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                val events = Events()
                val consumer = consumer("kafkakn-rebalance-lone-$armName-${randomSuffix()}")
                try {
                    consumer.subscribe(listOf(consumeTopic), events)
                    val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
                    while (events.log.isEmpty() && until.hasNotPassedNow()) consumer.poll(POLL)
                } finally {
                    consumer.close()
                }
                assertEquals(listOf("+[0]", "-[0]"), events.log, "a lone member of a one-partition topic")
                recordObservation("rebalance.lone", events.log.joinToString(" "))
            }
        }

    private fun consumer(group: String) =
        kafkaConsumer(
            ConsumerConfig(
                "bootstrap.servers" to bootstrap,
                "group.id" to group,
                "auto.offset.reset" to "earliest",
            ),
        )

    private suspend fun holdsAllAndSawTheEnd(
        consumer: KafkaConsumer,
        partitions: Int,
        end: Long,
        seen: List<String>,
    ): Boolean =
        consumer.assignment().size == partitions &&
            (0 until partitions).all { "$it:${end - 1}" in seen }

    private companion object {
        val POLL = 200.milliseconds
        val ASSIGNED_WITHIN = 30.seconds
        val REENTRY_WAIT = 5.seconds
        val STAY_AT_MOST: Duration = 2.minutes
    }
}
