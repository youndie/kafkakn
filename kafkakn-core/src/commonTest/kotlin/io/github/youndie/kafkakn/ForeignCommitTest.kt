package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-82](../../../../../../../docs/backlog/B-82-a-commit-of-a-partition-not-held-under-a-subscription.md): a commit of a
 * partition the member does not hold, under a subscription. The consumer contract (§2a) measured it with `assign`
 * only, where the group has no generation for the broker to judge the commit by, and left this case open.
 *
 * Two members of one group, each holding one of the topic's two partitions. Member A commits an offset for B's
 * partition while B holds it. What A's `commit` answers and what the group then resumes from are recorded as
 * observations, so `ci/harness/compare-arms.sh` says whether the arms agree. The broker's own view of the commit is
 * read by `ci/b-82/run.sh`. Both group protocols: classic, and KIP-848 (`group.protocol=consumer`).
 */
class ForeignCommitTest {
    @Test
    fun a_commit_of_a_partition_another_member_holds_classic() = measure(protocol = "classic")

    @Test
    fun a_commit_of_a_partition_another_member_holds_kip848() = measure(protocol = "consumer")

    private fun measure(protocol: String) =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-foreign-$protocol-$armName-${randomSuffix()}"
                val group = topic
                fill(topic)
                val a = member(group, protocol)
                val b = member(group, protocol)
                var outcome = "not tried"
                var foreign: TopicPartition? = null
                try {
                    a.subscribe(listOf(topic))
                    b.subscribe(listOf(topic))
                    // Both poll until each holds one partition, then until B has read all of its own.
                    var bRead = 0
                    val until = TimeSource.Monotonic.markNow() + WITHIN
                    while (until.hasNotPassedNow()) {
                        a.poll(POLL)
                        bRead += b.poll(POLL).size
                        if (a.assignment().size == 1 && b.assignment().size == 1 && bRead >= PER_PARTITION) break
                    }
                    check(a.assignment().size == 1 && b.assignment().size == 1) {
                        "not one partition each: A ${a.assignment()}, B ${b.assignment()}"
                    }
                    foreign = b.assignment().single()
                    outcome =
                        try {
                            a.commit(mapOf(foreign to FOREIGN_OFFSET))
                            "accepted"
                        } catch (cancelled: CancellationException) {
                            throw cancelled
                        } catch (refused: Exception) {
                            recordArmFact(
                                "foreign.$protocol.refusal",
                                "${refused::class.simpleName}: ${refused.message}",
                            )
                            "refused ${refused::class.simpleName}"
                        }
                } finally {
                    // B commits nothing of its own: what the group resumes from is whatever A's commit left.
                    a.close()
                    b.close()
                }
                val partition = checkNotNull(foreign)
                // What the group resumes from, read by a new member: its first record of that partition.
                val c = member(group, protocol)
                var first: Long? = null
                try {
                    c.subscribe(listOf(topic))
                    val until = TimeSource.Monotonic.markNow() + WITHIN
                    while (first == null && until.hasNotPassedNow()) {
                        first = c.poll(POLL).firstOrNull { it.partition == partition.partition }?.offset
                    }
                } finally {
                    c.close()
                }
                recordArmFact("foreign.$protocol.group", group)
                recordArmFact("foreign.$protocol.partition", partition.partition.toString())
                recordObservation("foreign.$protocol.commit", outcome)
                recordObservation("foreign.$protocol.resumed.from", first?.toString() ?: "nothing read")
            }
        }

    private fun member(
        group: String,
        protocol: String,
    ) = kafkaConsumer(
        ConsumerConfig(
            buildMap {
                put("bootstrap.servers", bootstrap)
                put("group.id", group)
                put("auto.offset.reset", "earliest")
                put("group.protocol", protocol)
                // Under the classic protocol the assignment is the client's, and range gives two members one each.
                if (protocol == "classic") put("partition.assignment.strategy", "range")
            },
        ),
    )

    private suspend fun fill(topic: String) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, 2, 1)))
        } finally {
            admin.close()
        }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap))
        try {
            // One at a time: a burst into a topic created a moment before is what B-79 found the Java client losing.
            for (index in 0 until PER_PARTITION) {
                for (partition in 0..1) {
                    producer.send(ProducerRecord(topic, "r-$index".encodeToByteArray(), partition = partition))
                }
            }
        } finally {
            producer.close()
        }
    }

    private companion object {
        const val PER_PARTITION = 20
        const val FOREIGN_OFFSET = 5L
        val POLL = 200.milliseconds
        val WITHIN = 60.seconds
    }
}
