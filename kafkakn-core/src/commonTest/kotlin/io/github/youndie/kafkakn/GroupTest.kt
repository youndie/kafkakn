package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-37](../../../../../../../docs/backlog/B-37-consumer-groups.md): consumer groups — subscribe, share
 * the partitions, hand them over, commit.
 *
 * **The promise is at-least-once, so the test is for LOSS**: every record written must be seen by some
 * member, and a record seen twice is allowed. The member that leaves does so the way a crash would —
 * it reads one last batch and closes without committing it — so the handover has something to deliver
 * again. Every member records what it saw as `partition:offset`; `ci/b-37/run.sh` holds the union
 * against the broker's end offsets, and the group's commits against `kafka-consumer-groups.sh`.
 *
 * The records are the third party's (`ci/harness/Records.java trickle`), written while the group forms,
 * so a split meets records rather than an empty topic.
 */
class GroupTest {
    @Test
    fun two_members_share_the_partitions_hand_them_over_and_lose_nothing() =
        runTest(timeout = TIMEOUT) {
            val topic = testEnv("KAFKAKN_GROUP_TOPIC") ?: consumeTopic
            val partitions = testEnv("KAFKAKN_GROUP_PARTITIONS")?.toInt() ?: 1
            val runFor = testEnv("KAFKAKN_GROUP_RUN_MS")?.toLong()?.milliseconds ?: DEFAULT_RUN
            val group = "kafkakn-group-$armName-${randomSuffix()}"
            val leaving = MemberLog()
            val staying = MemberLog()
            withContext(Dispatchers.Default) {
                coroutineScope {
                    launch { runMember(group, topic, runFor / 3, abandonLastBatch = true, leaving) }
                    launch { runMember(group, topic, runFor, abandonLastBatch = false, staying) }
                }
            }
            val seen = leaving.seen + staying.seen
            recordArmFact("group.id", group)
            recordArmFact("group.seen", seen.distinct().joinToString(";"))
            recordArmFact("group.twice", (seen.size - seen.distinct().size).toString())
            recordArmFact("group.abandoned", leaving.abandoned.joinToString(";"))
            recordArmFact("group.leaving.held", leaving.history())
            recordArmFact("group.staying.held", staying.history())

            assertTrue(
                leaving.held.any { it.isNotEmpty() },
                "the leaving member never held a partition: ${leaving.history()}",
            )
            assertEquals(partitions, staying.held.last().size, "after the handover: ${staying.history()}")
            if (partitions > 1) {
                // The split: at some moment the leaving member held some of the partitions, not all.
                assertTrue(
                    leaving.held.any { it.isNotEmpty() && it.size < partitions },
                    "the group never split: ${leaving.history()} / ${staying.history()}",
                )
            }
        }

    /**
     * One member of a group whose other member runs on the OTHER arm, in another process at the same
     * time — `ci/b-37/run.sh` starts both. Asked for through the environment, and only then: in a suite
     * run it records that it was not asked, and the run script that does ask refuses a member that says
     * so. It is the one test here whose subject exists only when a run arranges it.
     */
    @Test
    fun a_member_of_a_group_whose_other_member_is_the_other_arm() =
        runTest(timeout = TIMEOUT) {
            val group = testEnv("KAFKAKN_MIXED_GROUP")
            if (group == null) {
                recordArmFact("group.mixed", "not asked")
                return@runTest
            }
            val log = MemberLog()
            withContext(Dispatchers.Default) {
                runMember(
                    group = group,
                    topic = testEnv("KAFKAKN_MIXED_TOPIC") ?: error("KAFKAKN_MIXED_TOPIC"),
                    runFor = testEnv("KAFKAKN_MIXED_RUN_MS")!!.toLong().milliseconds,
                    abandonLastBatch = testEnv("KAFKAKN_MIXED_LEAVES") == "true",
                    log = log,
                )
            }
            recordArmFact("group.mixed", "member")
            recordArmFact("group.mixed.seen", log.seen.distinct().joinToString(";"))
            recordArmFact("group.mixed.held", log.history())
            recordArmFact("group.mixed.abandoned", log.abandoned.joinToString(";"))
            assertTrue(log.held.any { it.isNotEmpty() }, "this member never held a partition: ${log.history()}")
        }

    @Test
    fun subscribe_and_commit_need_a_group_the_caller_named_on_both_arms() =
        runTest(timeout = TIMEOUT) {
            // The native arm has a private group id of its own (B-36) so that librdkafka will assign;
            // a subscription joining it, or a commit landing in it, would be a group nobody asked for.
            val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap))
            try {
                val subscribe = assertFailsWith<IllegalStateException> { consumer.subscribe(listOf(consumeTopic)) }
                assertTrue(subscribe.message.orEmpty().contains("group.id"), subscribe.message)
                consumer.assign(listOf(TopicPartition(consumeTopic, 0)))
                val commit = assertFailsWith<IllegalStateException> { consumer.commit() }
                assertTrue(commit.message.orEmpty().contains("group.id"), commit.message)
            } finally {
                withContext(Dispatchers.Default) { consumer.close() }
            }
        }

    @Test
    fun a_seek_under_a_subscription_is_refused_on_both_arms() =
        runTest(timeout = TIMEOUT) {
            val consumer =
                kafkaConsumer(
                    ConsumerConfig(
                        "bootstrap.servers" to bootstrap,
                        "group.id" to "kafkakn-group-seek-${randomSuffix()}",
                    ),
                )
            try {
                consumer.subscribe(listOf(consumeTopic))
                assertFailsWith<IllegalStateException> {
                    consumer.seek(
                        TopicPartition(consumeTopic, 0),
                        SeekTo.Beginning,
                    )
                }
            } finally {
                withContext(Dispatchers.Default) { consumer.close() }
            }
        }

    /** What one member saw and held, in the order it happened. */
    private class MemberLog {
        val seen = mutableListOf<String>()
        val held = mutableListOf<List<TopicPartition>>()
        val abandoned = mutableListOf<String>()

        fun history() = held.joinToString("|") { taken -> taken.joinToString(",", "[", "]") { "${it.partition}" } }
    }

    private suspend fun runMember(
        group: String,
        topic: String,
        runFor: Duration,
        abandonLastBatch: Boolean,
        log: MemberLog,
    ) {
        val consumer =
            kafkaConsumer(
                ConsumerConfig(
                    "bootstrap.servers" to bootstrap,
                    "group.id" to group,
                    // A new group starts at `latest` by default on both arms, which would skip every
                    // record written before it joined - loss the test would then blame on the handover.
                    "auto.offset.reset" to "earliest",
                ),
            )
        try {
            consumer.subscribe(listOf(topic))
            val deadline = TimeSource.Monotonic.markNow() + runFor
            while (deadline.hasNotPassedNow()) {
                val batch = consumer.poll(POLL)
                note(consumer, log)
                log.seen += batch.map { "${it.partition}:${it.offset}" }
                consumer.commit()
            }
            if (abandonLastBatch) {
                // The crash: one more batch read - processed, as far as the caller is concerned - and
                // never committed. Whoever takes these partitions must deliver it again.
                val until = TimeSource.Monotonic.markNow() + LAST_BATCH_WAIT
                while (until.hasNotPassedNow()) {
                    val batch = consumer.poll(POLL)
                    if (batch.isNotEmpty()) {
                        // NOT added to `seen`: a crash is exactly the case where "returned" did not
                        // become "processed". The group's union must still cover these - which only
                        // redelivery to another member can do. Counting them as seen here made the loss
                        // check blind to whether they ever came back.
                        log.abandoned += batch.map { "${it.partition}:${it.offset}" }
                        break
                    }
                }
            }
        } finally {
            consumer.close()
        }
    }

    private suspend fun note(
        consumer: KafkaConsumer,
        log: MemberLog,
    ) {
        val now = consumer.assignment()
        if (log.held.lastOrNull() != now) log.held += now
    }

    private companion object {
        val TIMEOUT = 3.minutes
        val DEFAULT_RUN = 12.seconds
        val POLL = 200.milliseconds
        val LAST_BATCH_WAIT = 5.seconds
    }
}
