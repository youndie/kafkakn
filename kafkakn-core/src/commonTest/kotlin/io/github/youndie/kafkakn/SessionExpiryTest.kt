package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-65](../../../../../../../docs/backlog/B-65-onlost-when-the-session-expires.md): `onLost` when a member's
 * session expires. One member of a two-member group, asked for through the environment: `ci/b-65/run.sh` starts
 * one on each arm and freezes one of them with `SIGSTOP` for longer than `session.timeout.ms`, which stops its
 * heartbeats and its polls alike, then resumes it.
 *
 * Each member commits only on revocation, as B-50's do, so what a lost member processed and could not commit is
 * processed again by the next owner, and the runner counts it. `max.poll.interval.ms` stays at its default of
 * five minutes, so the only road out of the group is the session's: the one B-64 did not take.
 */
class SessionExpiryTest {
    @Test
    fun a_member_of_a_group_whose_session_may_expire() =
        runTest(timeout = 3.minutes) {
            val group = testEnv("KAFKAKN_SESSION_GROUP")
            if (group == null) {
                recordArmFact("session.member", "not asked")
                return@runTest
            }
            val topic = testEnv("KAFKAKN_SESSION_TOPIC") ?: error("KAFKAKN_SESSION_TOPIC")
            val end = testEnv("KAFKAKN_SESSION_END")!!.toLong()
            val name = testEnv("KAFKAKN_SESSION_NAME") ?: armName
            val heard = mutableListOf<String>()
            val seen = mutableListOf<String>()
            val held = mutableSetOf<Int>()
            val processed = mutableMapOf<TopicPartition, Long>()
            val strays = mutableListOf<String>()
            // Where each assignment started reading: "p:o@t" for the first record of each partition after an
            // onAssigned at wall time t. And what each revocation committed: "p:o@t". Together they say whether a
            // member that comes back starts from the group's commit or from its own old position.
            val firsts = mutableListOf<String>()
            val committed = mutableListOf<String>()
            val unread = mutableMapOf<Int, Long>()
            val listener =
                object : RebalanceListener {
                    override fun onAssigned(partitions: List<TopicPartition>) {
                        val at = wallClock()
                        heard += "+${partitions.map { it.partition }}@$at"
                        held += partitions.map { it.partition }
                        partitions.forEach { unread[it.partition] = at }
                    }

                    override fun onRevoked(
                        partitions: List<TopicPartition>,
                        scope: RebalanceScope,
                    ) {
                        val at = wallClock()
                        heard += "-${partitions.map { it.partition }}@$at"
                        val committing = processed.filterKeys { it in partitions }
                        scope.commit(committing)
                        committing.forEach { (partition, offset) -> committed += "${partition.partition}:$offset@$at" }
                        held -= partitions.map { it.partition }.toSet()
                        // Committed and gone: kept, it would be committed again on a later revocation of the same
                        // partition, over whatever its owner in between committed. It did, once: the stale 187 of a
                        // member given the partitions back at the end overwrote the other member's 300.
                        processed.keys.removeAll { it in partitions }
                    }

                    override fun onLost(partitions: List<TopicPartition>) {
                        // No scope: a member that was removed can no longer commit, and what it processed since
                        // its last commit is the next owner's to process again.
                        heard += "!${partitions.map { it.partition }}@${wallClock()}"
                        held -= partitions.map { it.partition }.toSet()
                        processed.keys.removeAll { it in partitions }
                    }
                }
            withContext(Dispatchers.Default) {
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to group,
                            "auto.offset.reset" to "earliest",
                            "session.timeout.ms" to SESSION_TIMEOUT_MS,
                        ),
                    )
                try {
                    consumer.subscribe(listOf(topic), listener)
                    val started = TimeSource.Monotonic.markNow()
                    while (true) {
                        check(started.elapsedNow() < STAY_AT_MOST) { "gave up: $heard" }
                        for (record in consumer.poll(POLL)) {
                            // A record of a partition this member does not hold, by its own listener's account:
                            // one fetched before the loss and handed over after it.
                            if (record.partition !in
                                held
                            ) {
                                strays += "${record.partition}:${record.offset}@${wallClock()}"
                            }
                            unread.remove(record.partition)?.let { at ->
                                firsts +=
                                    "${record.partition}:${record.offset}@$at"
                            }
                            seen += "${record.partition}:${record.offset}"
                            processed[TopicPartition(record.topic, record.partition)] = record.offset + 1
                        }
                        val holding = consumer.assignment()
                        if (started.elapsedNow() >= MIN_STAY && holding.isNotEmpty() &&
                            holding.all { consumer.position(it) >= end }
                        ) {
                            break
                        }
                    }
                } finally {
                    consumer.close()
                }
            }
            recordArmFact("session.$name.heard", heard.joinToString(" "))
            recordArmFact("session.$name.seen", seen.joinToString(";"))
            recordArmFact("session.$name.strays", strays.size.toString())
            recordArmFact("session.$name.stray.records", strays.joinToString(" "))
            recordArmFact("session.$name.firsts", firsts.joinToString(" "))
            recordArmFact("session.$name.committed", committed.joinToString(" "))
        }

    @Suppress("ktlint:kapkan:wall-clock", "orders events from members in separate processes against the freeze")
    private fun wallClock(): Long = Clock.System.now().toEpochMilliseconds()

    private companion object {
        const val SESSION_TIMEOUT_MS = "10000"
        val POLL = 200.milliseconds
        val MIN_STAY = 75.seconds
        val STAY_AT_MOST = 150.seconds
    }
}
