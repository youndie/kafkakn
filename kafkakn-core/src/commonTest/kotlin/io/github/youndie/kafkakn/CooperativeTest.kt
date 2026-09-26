package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-55](../../../../../../../docs/backlog/B-55-cooperative-rebalancing.md): cooperative rebalancing, with
 * one portable spelling of `partition.assignment.strategy`.
 */
class CooperativeTest {
    @Test
    fun a_strategy_only_one_arm_understands_is_refused_at_construction() {
        for (value in listOf(
            // The Java client's own spelling: a class name librdkafka does not know.
            "org.apache.kafka.clients.consumer.RangeAssignor",
            // The Java client's eager sticky assignor, which librdkafka does not have.
            "sticky",
            // Cooperative next to an eager assignor: one protocol at a time.
            "cooperative-sticky,range",
        )) {
            assertFailsWith<IllegalArgumentException>(value) {
                kafkaConsumer(
                    ConsumerConfig(
                        "bootstrap.servers" to bootstrap,
                        "group.id" to "g",
                        "partition.assignment.strategy" to value,
                    ),
                )
            }
        }
        recordObservation("cooperative.refused", "3")
    }

    @Test
    fun the_portable_spellings_are_accepted() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                for (value in listOf("range", "roundrobin", "cooperative-sticky", "range,roundrobin")) {
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to "g",
                            "partition.assignment.strategy" to value,
                        ),
                    ).close()
                }
            }
        }

    /**
     * One member of a cooperative group of three, asked for through the environment (as B-37's and B-50's
     * members are): `ci/b-55/run.sh` starts one on each arm, and a third later. Each commits only on
     * revocation. It stays until every partition it holds has its position at the end, and at least
     * [MIN_STAY], so the third member's arrival happens while all three are in. A position rather than a
     * record seen, because a partition inherited from a member that left was read there, not here.
     */
    @Test
    fun a_member_of_a_cooperative_group() =
        runTest(timeout = 3.minutes) {
            val group = testEnv("KAFKAKN_COOP_GROUP")
            if (group == null) {
                recordArmFact("coop.member", "not asked")
                return@runTest
            }
            val topic = testEnv("KAFKAKN_COOP_TOPIC") ?: error("KAFKAKN_COOP_TOPIC")
            val end = testEnv("KAFKAKN_COOP_END")!!.toLong()
            val name = testEnv("KAFKAKN_COOP_NAME") ?: armName
            val joinAfter = testEnv("KAFKAKN_COOP_JOIN_AFTER_MS")?.toLong()?.milliseconds
            // B-57: the same member under the KIP-848 protocol, where the broker assigns and the strategy key
            // is refused; `ci/b-57/run.sh` asks for it.
            val protocol = testEnv("KAFKAKN_COOP_PROTOCOL") ?: "classic"
            val events = mutableListOf<String>()
            // When each event happened, on the machine's clock: the members are separate processes, and only
            // a wall clock orders their events against each other.
            val times = mutableListOf<Long>()
            val seen = mutableListOf<String>()
            val processed = mutableMapOf<TopicPartition, Long>()
            val listener =
                object : RebalanceListener {
                    override fun onAssigned(partitions: List<TopicPartition>) {
                        events += "+${partitions.map { it.partition }}"
                        times += wallClock()
                    }

                    override fun onRevoked(
                        partitions: List<TopicPartition>,
                        scope: RebalanceScope,
                    ) {
                        events += "-${partitions.map { it.partition }}"
                        times += wallClock()
                        scope.commit(processed.filterKeys { it in partitions })
                        // Committed and gone (B-69): kept, it would be committed again on a later revocation of the
                        // same partition, over whatever its owner in between committed. B-65's member did exactly
                        // that once, a stale 187 over another member's 300.
                        processed.keys.removeAll { it in partitions }
                    }

                    override fun onLost(partitions: List<TopicPartition>) {
                        events += "!${partitions.map { it.partition }}"
                        times += wallClock()
                        // Lost, not committed: the next owner starts from the group's commit, not from this.
                        processed.keys.removeAll { it in partitions }
                    }
                }
            withContext(Dispatchers.Default) {
                // On the real dispatcher: inside runTest a delay runs on virtual time and is skipped, which
                // made the member meant to join 30 s late join at the same millisecond as the other.
                joinAfter?.let { delay(it) }
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to group,
                            "auto.offset.reset" to "earliest",
                            if (protocol == "consumer") {
                                "group.protocol" to "consumer"
                            } else {
                                "partition.assignment.strategy" to "cooperative-sticky"
                            },
                        ),
                    )
                try {
                    consumer.subscribe(listOf(topic), listener)
                    val started = TimeSource.Monotonic.markNow()
                    while (true) {
                        check(started.elapsedNow() < STAY_AT_MOST) { "gave up: $events" }
                        for (record in consumer.poll(POLL)) {
                            seen += "${record.partition}:${record.offset}"
                            processed[TopicPartition(record.topic, record.partition)] = record.offset + 1
                        }
                        val held = consumer.assignment()
                        if (started.elapsedNow() >= MIN_STAY && held.isNotEmpty() &&
                            held.all { consumer.position(it) >= end }
                        ) {
                            break
                        }
                    }
                } finally {
                    consumer.close()
                }
            }
            recordArmFact("coop.$name.events", events.joinToString(" "))
            recordArmFact("coop.$name.seen", seen.joinToString(";"))
            recordArmFact("coop.$name.times", times.joinToString(" "))
            assertTrue(events.any { it.startsWith("+") }, "never assigned: $events")
        }

    @Suppress("ktlint:kapkan:wall-clock", "orders events from members in separate processes on one machine")
    private fun wallClock(): Long = Clock.System.now().toEpochMilliseconds()

    private companion object {
        val POLL = 200.milliseconds
        val MIN_STAY = 40.seconds
        val STAY_AT_MOST = 150.seconds
    }
}
