package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

/**
 * [B-59](../../../../../../../docs/backlog/B-59-consumer-group-offsets-and-lag.md): a group's committed
 * offsets, and a partition's start, end and offset for a time, read by the admin client.
 *
 * Each arm makes a topic of its own with three partitions: five records in partition 0 and three in
 * partition 1, a second apart from [BASE], and none in partition 2. The records carry timestamps from 2001,
 * so the topic keeps them for ever (`retention.ms=-1`) instead of dropping them at the first retention check.
 * A group commits two of the partitions without ever joining. `ci/b-59/run.sh` holds each arm's answers
 * against `kafka-consumer-groups.sh --describe` and `kafka-get-offsets.sh` on that arm's topic.
 */
class AdminOffsetsTest {
    @Test
    fun a_groups_committed_offsets_are_read_from_outside_the_group() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-group-offsets")
                val group = topic
                val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap, "group.id" to group))
                try {
                    consumer.assign(listOf(TopicPartition(topic, 0), TopicPartition(topic, 1)))
                    consumer.commit(mapOf(TopicPartition(topic, 1) to 3L, TopicPartition(topic, 0) to 4L))
                } finally {
                    consumer.close()
                }
                withAdmin { admin ->
                    val committed = admin.listConsumerGroupOffsets(group)
                    assertEquals(
                        mapOf(TopicPartition(topic, 0) to 4L, TopicPartition(topic, 1) to 3L),
                        committed,
                        "the group's commits, and nothing for the partition it never committed",
                    )
                    assertEquals(listOf(0, 1), committed.keys.map { it.partition }, "in partition order")
                    recordArmFact("admin.offsets.group", group)
                    recordArmFact("admin.offsets.committed", rendered(committed))
                    recordObservation("admin.offsets.committed", rendered(committed))

                    val lag = admin.listOffsets(committed.keys.toList(), OffsetSpec.Latest)
                    recordObservation(
                        "admin.offsets.lag",
                        committed.entries.joinToString(" ") { (at, offset) -> "${at.partition}:${lag[at]!! - offset}" },
                    )

                    val missing = outcome { admin.listConsumerGroupOffsets("kafkakn-no-such-group-${randomSuffix()}") }
                    assertEquals("{}", missing, "a group that does not exist")
                    recordObservation("admin.offsets.missing.group", missing)
                }
            }
        }

    @Test
    fun a_partitions_start_end_and_offset_for_a_time_are_the_brokers() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-list-offsets")
                val partitions = (0 until PARTITIONS).map { TopicPartition(topic, it) }
                withAdmin { admin ->
                    val asked =
                        listOf(
                            "earliest" to OffsetSpec.Earliest,
                            "latest" to OffsetSpec.Latest,
                            // Between the third and the fourth record of partition 0: the fourth is offset 3.
                            "between" to OffsetSpec.Timestamp(BASE + 2 * STEP + STEP / 2),
                            // Exactly the second record's time: at or after, so that record itself.
                            "exact" to OffsetSpec.Timestamp(BASE + STEP),
                            "before" to OffsetSpec.Timestamp(0),
                            "after" to OffsetSpec.Timestamp(BASE + 100 * STEP),
                        )
                    val answers =
                        asked.associate { (name, spec) ->
                            name to
                                rendered(admin.listOffsets(partitions, spec))
                        }
                    assertEquals(
                        mapOf(
                            "earliest" to "0:0 1:0 2:0",
                            "latest" to "0:5 1:3 2:0",
                            "between" to "0:3 1:none 2:none",
                            "exact" to "0:1 1:1 2:none",
                            "before" to "0:0 1:0 2:none",
                            "after" to "0:none 1:none 2:none",
                        ),
                        answers,
                    )
                    recordArmFact("admin.offsets.topic", topic)
                    answers.forEach { (name, answer) ->
                        recordArmFact("admin.offsets.$name", answer)
                        recordObservation("admin.offsets.$name", answer)
                    }
                    asked.forEach { (name, spec) ->
                        if (spec is OffsetSpec.Timestamp) {
                            recordArmFact(
                                "admin.offsets.$name.at",
                                spec.timestamp.toString(),
                            )
                        }
                    }
                }
            }
        }

    /** A new topic of [PARTITIONS] partitions: five records in 0, three in 1, none in 2, a [STEP] apart from [BASE]. */
    private suspend fun seededTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        withAdmin { it.createTopics(listOf(NewTopic(topic, PARTITIONS, 1, mapOf("retention.ms" to "-1")))) }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            for ((partition, count) in listOf(0 to 5, 1 to 3)) {
                repeat(count) { index ->
                    producer.send(
                        ProducerRecord(
                            topic,
                            "$partition:$index".encodeToByteArray(),
                            partition = partition,
                            timestamp = BASE + index * STEP,
                        ),
                    )
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

    /** `0:4 1:3`: partition numbers only, so that the two arms' topics render alike. */
    private fun rendered(offsets: Map<TopicPartition, Long?>): String =
        offsets.entries.joinToString(" ") { (at, offset) -> "${at.partition}:${offset ?: "none"}" }

    /** What a call returned, rendered, or the exception it threw: the same text on both arms, or a finding. */
    private suspend fun outcome(call: suspend () -> Map<TopicPartition, Long>): String =
        try {
            call().let { if (it.isEmpty()) "{}" else rendered(it) }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: Exception) {
            "threw ${thrown::class.simpleName}"
        }

    private companion object {
        const val PARTITIONS = 3
        const val BASE = 1_000_000_000_000L
        const val STEP = 1_000L
    }
}
