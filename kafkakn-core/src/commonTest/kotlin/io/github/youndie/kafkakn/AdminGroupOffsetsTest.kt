package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-60](../../../../../../../docs/backlog/B-60-reset-and-delete-group-offsets.md): move an empty group's
 * offsets, delete them, and delete the group, through the admin client; and the broker's refusal while the
 * group has a member.
 *
 * Each arm makes a topic of its own: ten records in partition 0, four in partition 1. `ci/b-60/run.sh` holds
 * the moved offsets against `kafka-consumer-groups.sh --describe` and the deleted group against `--list`.
 */
class AdminGroupOffsetsTest {
    @Test
    fun an_empty_groups_offsets_are_moved_and_a_member_that_joins_reads_from_there() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-reset")
                val group = topic
                commitAsMember(group, mapOf(TopicPartition(topic, 0) to 2L))
                withAdmin { admin ->
                    admin.alterConsumerGroupOffsets(group, mapOf(TopicPartition(topic, 0) to MOVED_TO))
                    assertEquals(mapOf(TopicPartition(topic, 0) to MOVED_TO), admin.listConsumerGroupOffsets(group))
                }
                // A member that joins afterwards, commits nothing, and reads from where the group was moved.
                val first = firstReadBy(group, topic)
                assertEquals(MOVED_TO, first, "the first offset a member read after the move")
                recordArmFact("admin.reset.group", group)
                recordObservation("admin.reset.first", first.toString())
            }
        }

    @Test
    fun a_group_with_an_active_member_is_refused_with_one_exception_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-refused")
                val group = topic
                val member =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to group,
                            "auto.offset.reset" to "earliest",
                        ),
                    )
                try {
                    member.subscribe(listOf(topic))
                    val until = TimeSource.Monotonic.markNow() + ASSIGNED_WITHIN
                    while (member.assignment().isEmpty() && until.hasNotPassedNow()) member.poll(POLL)
                    check(member.assignment().isNotEmpty()) { "the member was never assigned" }
                    member.commit(mapOf(TopicPartition(topic, 0) to 1L))
                    withAdmin { admin ->
                        val refusals =
                            listOf(
                                "alter" to
                                    outcome {
                                        admin.alterConsumerGroupOffsets(
                                            group,
                                            mapOf(TopicPartition(topic, 0) to 5L),
                                        )
                                    },
                                "delete.offsets" to
                                    outcome {
                                        admin.deleteConsumerGroupOffsets(
                                            group,
                                            listOf(TopicPartition(topic, 0)),
                                        )
                                    },
                                "delete.group" to outcome { admin.deleteConsumerGroups(listOf(group)) },
                            )
                        refusals.forEach { (what, said) ->
                            recordArmFact("admin.refused.$what.said", said)
                            recordObservation("admin.refused.$what", said.substringBefore(":"))
                        }
                        assertEquals(
                            refusals.map { it.first to "threw GroupNotEmptyException" },
                            refusals.map { (what, said) -> what to said.substringBefore(":") },
                        )
                        assertEquals(
                            mapOf(TopicPartition(topic, 0) to 1L),
                            admin.listConsumerGroupOffsets(group),
                            "the member's commit, untouched by the refused calls",
                        )
                    }
                } finally {
                    member.close()
                }
            }
        }

    @Test
    fun an_empty_groups_offsets_and_then_the_group_are_deleted() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-deleted")
                val group = topic
                commitAsMember(group, mapOf(TopicPartition(topic, 0) to 3L, TopicPartition(topic, 1) to 2L))
                withAdmin { admin ->
                    admin.deleteConsumerGroupOffsets(group, listOf(TopicPartition(topic, 0)))
                    val left = admin.listConsumerGroupOffsets(group)
                    assertEquals(mapOf(TopicPartition(topic, 1) to 2L), left, "only the partition not deleted")
                    recordObservation(
                        "admin.deleted.offsets.left",
                        left.entries.joinToString { "${it.key.partition}:${it.value}" },
                    )

                    admin.deleteConsumerGroups(listOf(group))
                    assertTrue(admin.listConsumerGroups().none { it.groupId == group }, "a deleted group is not listed")
                    assertEquals(emptyMap(), admin.listConsumerGroupOffsets(group), "nor has it any offsets")
                    recordArmFact("admin.deleted.group", group)

                    val missing =
                        outcome { admin.deleteConsumerGroups(listOf("kafkakn-no-such-group-${randomSuffix()}")) }
                    // Refused on both arms, each in its client's own type (GROUP_ID_NOT_FOUND): recorded, not promised.
                    recordArmFact("admin.deleted.missing.said", missing)
                    recordObservation("admin.deleted.missing", if (missing.startsWith("threw")) "refused" else missing)
                }
            }
        }

    /** A member that commits [offsets] and leaves: the group is then empty, with those commits. */
    private suspend fun commitAsMember(
        group: String,
        offsets: Map<TopicPartition, Long>,
    ) {
        val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap, "group.id" to group))
        try {
            consumer.assign(offsets.keys.toList())
            consumer.commit(offsets)
        } finally {
            consumer.close()
        }
    }

    /** The offset of the first record of partition 0 a new member of [group] reads. It commits nothing. */
    private suspend fun firstReadBy(
        group: String,
        topic: String,
    ): Long {
        val consumer =
            kafkaConsumer(
                ConsumerConfig(
                    "bootstrap.servers" to bootstrap,
                    "group.id" to group,
                    "auto.offset.reset" to "earliest",
                ),
            )
        try {
            consumer.subscribe(listOf(topic))
            val until = TimeSource.Monotonic.markNow() + READ_WITHIN
            while (until.hasNotPassedNow()) {
                consumer.poll(POLL).firstOrNull { it.partition == 0 }?.let { return it.offset }
            }
            error("nothing read from partition 0 in $READ_WITHIN")
        } finally {
            consumer.close()
        }
    }

    /** A new topic of two partitions: ten records in 0, four in 1. */
    private suspend fun seededTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        withAdmin { it.createTopics(listOf(NewTopic(topic, 2, 1))) }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            for ((partition, count) in listOf(0 to 10, 1 to 4)) {
                repeat(count) { index ->
                    producer.send(ProducerRecord(topic, "$partition:$index".encodeToByteArray(), partition = partition))
                }
            }
        } finally {
            producer.close()
        }
        return topic
    }

    private suspend fun withAdmin(use: suspend (KafkaAdmin) -> Unit) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            use(admin)
        } finally {
            admin.close()
        }
    }

    /** "done", or "threw <type>: <message>": the type is compared across the arms, the message recorded. */
    private suspend fun outcome(call: suspend () -> Unit): String =
        try {
            call()
            "done"
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            // By type, not by name alone: the Java client has a GroupNotEmptyException of its own, and a caller
            // catching kafkakn's would miss it. A mutant that left the Java one unmapped passed a name check.
            val type = thrown::class.simpleName + if (thrown is GroupNotEmptyException) "" else " (the client's own)"
            "threw $type: ${thrown.message}"
        }

    private companion object {
        const val MOVED_TO = 7L
        val POLL = 200.milliseconds
        val ASSIGNED_WITHIN = 30.seconds
        val READ_WITHIN = 30.seconds
    }
}
