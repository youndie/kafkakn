package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-56](../../../../../../../docs/backlog/B-56-static-membership.md): static membership. A member with a
 * `group.instance.id` that is closed and reopened within the session timeout gets its partitions back, and
 * the group does not rebalance.
 *
 * Two members of one group read a topic of two partitions: a stayer, which polls throughout with a
 * rebalance listener, and a restarter, which is closed and reopened. What the stayer's listener hears while
 * the restarter is away and back is the member's view of a rebalance; `ci/b-56/run.sh` adds the broker's,
 * its own log of the group's generations over the same window, since `kafka-consumer-groups.sh` prints no
 * generation for a classic group.
 */
class StaticMembershipTest {
    @Test
    fun a_static_member_that_restarts_gets_its_partitions_back_without_a_rebalance() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val restart = restartWhileAnotherWatches(static = true)
                recordArmFact("static.restart.group", restart.group)
                recordArmFact("static.restart.window", "${restart.from} ${restart.to}")
                recordArmFact("static.restart.partitions", "${restart.before} ${restart.after}")
                recordObservation("static.restart.heard", restart.heard.joinToString(" ").ifEmpty { "nothing" })
                recordObservation("static.restart.same.partitions", (restart.before == restart.after).toString())
                assertEquals(emptyList(), restart.heard, "what the other member heard while this one restarted")
                assertEquals(restart.before, restart.after, "the partitions before and after the restart")
            }
        }

    /**
     * The positive control: the same restart without `group.instance.id` is a leave and a join, and the other
     * member hears both. Without it, a watcher that heard nothing would prove nothing.
     */
    @Test
    fun a_dynamic_member_that_restarts_makes_the_group_rebalance() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val restart = restartWhileAnotherWatches(static = false)
                recordArmFact("static.dynamic.group", restart.group)
                recordArmFact("static.dynamic.window", "${restart.from} ${restart.to}")
                recordObservation("static.dynamic.heard.anything", restart.heard.isNotEmpty().toString())
                assertTrue(restart.heard.isNotEmpty(), "a dynamic member's restart is a rebalance the other hears")
            }
        }

    /**
     * Two live members with one `group.instance.id`: the broker keeps the newer and fences the older, whose
     * next call is refused (`FENCED_INSTANCE_ID`).
     */
    @Test
    fun a_second_member_with_the_same_instance_id_fences_the_first() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = twoPartitionTopic("kafkakn-fenced")
                val group = topic
                val first = member(group, "$group-same")
                val second = member(group, "$group-same")
                try {
                    first.subscribe(listOf(topic))
                    pollUntilAssigned(first)
                    second.subscribe(listOf(topic))
                    pollUntilAssigned(second)
                    val refused = outcome { repeatFor(FENCED_WITHIN) { first.poll(POLL) } }
                    recordArmFact("static.fenced.first.said", refused)
                    recordObservation("static.fenced.first", refused.substringBefore(":"))
                    recordObservation("static.fenced.second.holds", second.assignment().size.toString())
                    assertEquals("threw ConsumerFencedException", refused.substringBefore(":"), "the older member")
                    assertEquals(PARTITIONS, second.assignment().size, "the newer member holds the partitions")
                } finally {
                    closeQuietly(first)
                    closeQuietly(second)
                }
            }
        }

    /**
     * [B-66](../../../../../../../docs/backlog/B-66-a-fenced-static-members-commit.md): a fenced member's commit.
     * The first member reads and commits, the second takes its `group.instance.id` and commits its own offsets,
     * and the first, not knowing, commits what it processed: both forms of `commit`, before its `poll` has told
     * it anything. The broker must refuse both, the same way on both arms, and the group's offsets must stay the
     * second member's; `ci/b-66/run.sh` reads them with `kafka-consumer-groups.sh --describe` too.
     */
    @Test
    fun a_fenced_members_commit_is_refused_and_changes_nothing() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = twoPartitionTopic("kafkakn-fenced-commit")
                val group = topic
                val p0 = TopicPartition(topic, 0)
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap))
                try {
                    repeat(RECORDS) { producer.send(ProducerRecord(topic, "$it".encodeToByteArray(), partition = 0)) }
                } finally {
                    producer.close()
                }
                val first = member(group, "$group-same")
                val second = member(group, "$group-same")
                try {
                    first.subscribe(listOf(topic))
                    val read = mutableListOf<Long>()
                    val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
                    while (read.size < RECORDS && until.hasNotPassedNow()) read += first.poll(POLL).map { it.offset }
                    first.commit(mapOf(p0 to FIRST_COMMITTED))
                    second.subscribe(listOf(topic))
                    pollUntilAssigned(second)
                    second.commit(mapOf(p0 to SECOND_COMMITTED))

                    val explicit = outcome { first.commit(mapOf(p0 to FENCED_WOULD_COMMIT)) }
                    val positions = outcome { first.commit() }
                    recordArmFact("static.fenced.commit.explicit.said", explicit)
                    recordArmFact("static.fenced.commit.positions.said", positions)
                    recordArmFact("static.fenced.commit.group", group)
                    recordObservation("static.fenced.commit.explicit", explicit.substringBefore(":"))
                    recordObservation("static.fenced.commit.positions", positions.substringBefore(":"))

                    val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                    val held =
                        try {
                            admin.listConsumerGroupOffsets(group)
                        } finally {
                            admin.close()
                        }
                    recordObservation(
                        "static.fenced.commit.group.offsets",
                        held.entries.joinToString { "${it.key.partition}:${it.value}" },
                    )
                    assertEquals(
                        mapOf(p0 to SECOND_COMMITTED),
                        held,
                        "the group's offsets, after the fenced member's commits",
                    )
                    assertEquals(
                        listOf("threw ConsumerFencedException", "threw ConsumerFencedException"),
                        listOf(explicit, positions).map { it.substringBefore(":") },
                        "both forms of commit on a fenced member",
                    )
                } finally {
                    closeQuietly(first)
                    closeQuietly(second)
                }
            }
        }

    /**
     * One member of a mixed group, asked for through the environment (as B-55's are): `ci/b-56/run.sh` starts
     * a stayer on one arm and a restarter on the other, and holds the stayer's events against the restarter's
     * window. Both are static.
     */
    @Test
    fun a_static_member_of_a_mixed_group() =
        runTest(timeout = 3.minutes) {
            val group = testEnv("KAFKAKN_STATIC_GROUP")
            if (group == null) {
                recordArmFact("static.mixed", "not asked")
                return@runTest
            }
            val topic = testEnv("KAFKAKN_STATIC_TOPIC") ?: error("KAFKAKN_STATIC_TOPIC")
            val role = testEnv("KAFKAKN_STATIC_ROLE") ?: error("KAFKAKN_STATIC_ROLE")
            withContext(Dispatchers.Default) {
                when (role) {
                    "stay" -> {
                        val heard = mutableListOf<String>()
                        val consumer = member(group, "$group-stayer")
                        try {
                            consumer.subscribe(listOf(topic), recorder { heard += "$it@${wallClock()}" })
                            repeatFor(STAY_FOR) { consumer.poll(POLL) }
                        } finally {
                            consumer.close()
                        }
                        recordArmFact("static.mixed.stayer.heard", heard.joinToString(" "))
                    }

                    "restart" -> {
                        val restarter = member(group, "$group-restarter")
                        val before: List<Int>
                        try {
                            restarter.subscribe(listOf(topic))
                            before = pollUntilAssigned(restarter)
                            repeatFor(SETTLE) { restarter.poll(POLL) }
                        } finally {
                            restarter.close()
                        }
                        val from = wallClock()
                        val (after, to) = rejoin(group, topic)
                        recordArmFact("static.mixed.restarter.window", "$from $to")
                        recordArmFact("static.mixed.restarter.partitions", "$before $after")
                    }

                    else -> {
                        error("KAFKAKN_STATIC_ROLE is stay or restart, not $role")
                    }
                }
            }
        }

    private class Restart(
        val group: String,
        val before: List<Int>,
        val after: List<Int>,
        val heard: List<String>,
        val from: Long,
        val to: Long,
    )

    /**
     * A stayer polls throughout; a restarter joins, settles, is closed, reopened and watched for [WATCH]. What
     * the stayer's listener heard from the moment the restarter closed until the watch ended is returned.
     */
    private suspend fun restartWhileAnotherWatches(static: Boolean): Restart =
        coroutineScope {
            val topic = twoPartitionTopic(if (static) "kafkakn-static" else "kafkakn-dynamic")
            val group = topic
            val heard = mutableListOf<String>()
            val listening = CompletableDeferred<Unit>()
            val stop = CompletableDeferred<Unit>()
            // A deferred rather than a plain flag: set here, read on the stayer's thread.
            val windowOpen = CompletableDeferred<Unit>()
            // Shut before the stayer stops: its own close revokes what it holds, and that is not the restart's.
            val windowShut = CompletableDeferred<Unit>()
            val stayer =
                launch {
                    val consumer = member(group, if (static) "$group-stayer" else null)
                    try {
                        consumer.subscribe(
                            listOf(topic),
                            recorder {
                                if (windowOpen.isCompleted &&
                                    !windowShut.isCompleted
                                ) {
                                    heard += it
                                }
                            },
                        )
                        pollUntilAssigned(consumer)
                        listening.complete(Unit)
                        while (!stop.isCompleted) consumer.poll(POLL)
                    } finally {
                        consumer.close()
                    }
                }
            listening.await()
            val instance = if (static) "$group-restarter" else null
            val restarter = member(group, instance)
            val before: List<Int>
            try {
                restarter.subscribe(listOf(topic))
                before = pollUntilAssigned(restarter)
                // Until the stayer has taken its share: the join above rebalanced the group, as a join does.
                repeatFor(SETTLE) { restarter.poll(POLL) }
            } finally {
                windowOpen.complete(Unit)
                restarter.close()
            }
            val from = wallClock()
            val (after, to) = rejoin(group, topic, instance)
            windowShut.complete(Unit)
            stop.complete(Unit)
            stayer.join()
            Restart(group, before, after, heard.toList(), from, to)
        }

    /** Reopens the restarter after [AWAY], polls until assigned, then keeps it for [WATCH]. */
    private suspend fun rejoin(
        group: String,
        topic: String,
        instance: String? = "$group-restarter",
    ): Pair<List<Int>, Long> {
        delay(AWAY)
        val back = member(group, instance)
        try {
            back.subscribe(listOf(topic))
            val after = pollUntilAssigned(back)
            repeatFor(WATCH) { back.poll(POLL) }
            return after to wallClock()
        } finally {
            back.close()
        }
    }

    private fun recorder(heard: (String) -> Unit) =
        object : RebalanceListener {
            override fun onAssigned(partitions: List<TopicPartition>) = heard("+${partitions.map { it.partition }}")

            override fun onRevoked(
                partitions: List<TopicPartition>,
                scope: RebalanceScope,
            ) = heard("-${partitions.map { it.partition }}")

            override fun onLost(partitions: List<TopicPartition>) = heard("!${partitions.map { it.partition }}")
        }

    private fun member(
        group: String,
        instance: String?,
    ): KafkaConsumer =
        kafkaConsumer(
            ConsumerConfig(
                buildMap {
                    put("bootstrap.servers", bootstrap)
                    put("group.id", group)
                    put("auto.offset.reset", "earliest")
                    if (instance != null) put("group.instance.id", instance)
                },
            ),
        )

    /** Polls until the member holds something, and returns what, by partition number. */
    private suspend fun pollUntilAssigned(consumer: KafkaConsumer): List<Int> {
        val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
        while (consumer.assignment().isEmpty() && until.hasNotPassedNow()) consumer.poll(POLL)
        val held = consumer.assignment().map { it.partition }.sorted()
        check(held.isNotEmpty()) { "never assigned in $ASSIGNED_WITHIN" }
        return held
    }

    private suspend fun repeatFor(
        duration: Duration,
        action: suspend () -> Unit,
    ) {
        val until = TimeSource.Monotonic.markNow() + duration
        while (until.hasNotPassedNow()) action()
    }

    private suspend fun twoPartitionTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, PARTITIONS, 1)))
        } finally {
            admin.close()
        }
        return topic
    }

    private suspend fun closeQuietly(consumer: KafkaConsumer) {
        try {
            consumer.close()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (refused: Exception) {
            // A fenced member's close may itself be refused: recorded, since it is how each client ends a fenced member.
            recordArmFact("static.fenced.close", "${refused::class.simpleName}: ${refused.message}")
        }
    }

    /** "done", or "threw <type>: <message>". */
    private suspend fun outcome(call: suspend () -> Unit): String =
        try {
            call()
            "done"
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            "threw ${thrown::class.simpleName}: ${thrown.message}"
        }

    @Suppress("ktlint:kapkan:wall-clock", "the broker's log is cut to this window by docker logs --since/--until")
    private fun wallClock(): Long = Clock.System.now().toEpochMilliseconds()

    private companion object {
        const val PARTITIONS = 2
        const val RECORDS = 10
        const val FIRST_COMMITTED = 4L
        const val SECOND_COMMITTED = 6L
        const val FENCED_WOULD_COMMIT = 9L
        val POLL = 200.milliseconds
        val ASSIGNED_WITHIN = 30.seconds
        val SETTLE = 5.seconds
        val AWAY = 2.seconds
        val WATCH = 10.seconds
        val FENCED_WITHIN = 20.seconds
        val STAY_FOR = 60.seconds
    }
}
