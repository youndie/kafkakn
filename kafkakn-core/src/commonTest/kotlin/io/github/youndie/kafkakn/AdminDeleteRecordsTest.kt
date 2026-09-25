package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.minutes

/**
 * [B-63](../../../../../../../docs/backlog/B-63-delete-records.md): delete records before an offset. Each arm
 * deletes on a topic of its own: ten records in partition 0, five in partition 1. `ci/b-63/run.sh` holds the
 * returned low watermarks against `kafka-get-offsets.sh --time -2`.
 */
class AdminDeleteRecordsTest {
    @Test
    fun records_before_an_offset_are_deleted_and_the_low_watermark_is_what_the_broker_reports() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-deleted-records")
                val p0 = TopicPartition(topic, 0)
                val p1 = TopicPartition(topic, 1)
                withAdmin { admin ->
                    val first = admin.deleteRecords(mapOf(p1 to 0L, p0 to 4L))
                    assertEquals(mapOf(p0 to 4L, p1 to 0L), first, "the low watermarks, in partition order")
                    assertEquals(listOf(0, 1), first.keys.map { it.partition })
                    assertEquals(mapOf(p0 to 4L, p1 to 0L), admin.listOffsets(listOf(p0, p1), OffsetSpec.Earliest))

                    // Behind the low watermark: nothing more to delete, and the answer is what is, not what was asked.
                    val behind = admin.deleteRecords(mapOf(p0 to 2L))
                    assertEquals(mapOf(p0 to 4L), behind, "asked for 2, the partition already starts at 4")

                    // Up to the end: the partition is empty, and starts where it ends.
                    val emptied = admin.deleteRecords(mapOf(p0 to 10L))
                    assertEquals(mapOf(p0 to 10L), emptied)
                    assertEquals(
                        mapOf(p0 to 10L),
                        admin.listOffsets(listOf(p0), OffsetSpec.Latest),
                        "the end is where it was",
                    )

                    recordArmFact("records.topic", topic)
                    recordArmFact("records.watermarks", "0:${emptied.getValue(p0)} 1:${first.getValue(p1)}")
                    recordObservation("records.first", render(first))
                    recordObservation("records.behind", render(behind))
                    recordObservation("records.emptied", render(emptied))
                }
            }
        }

    @Test
    fun an_offset_past_the_end_is_refused_alike_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = seededTopic("kafkakn-deleted-past-end")
                withAdmin { admin ->
                    val said = outcome { admin.deleteRecords(mapOf(TopicPartition(topic, 1) to 6L)) }
                    recordArmFact("records.refused.past.end.said", said)
                    recordObservation("records.refused.past.end", said.substringBefore(":"))
                    assertEquals("threw IllegalArgumentException", said.substringBefore(":"))
                    assertEquals(
                        mapOf(TopicPartition(topic, 1) to 0L),
                        admin.listOffsets(listOf(TopicPartition(topic, 1)), OffsetSpec.Earliest),
                        "nothing deleted",
                    )
                }
            }
        }

    /** Ten records in partition 0, five in partition 1. */
    private suspend fun seededTopic(prefix: String): String {
        val topic = "$prefix-$armName-${randomSuffix()}"
        withAdmin { it.createTopics(listOf(NewTopic(topic, 2, 1))) }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            for ((partition, count) in listOf(0 to 10, 1 to 5)) {
                repeat(
                    count,
                ) { producer.send(ProducerRecord(topic, "$partition:$it".encodeToByteArray(), partition = partition)) }
            }
        } finally {
            producer.close()
        }
        return topic
    }

    private fun render(watermarks: Map<TopicPartition, Long>): String =
        watermarks.entries.joinToString(" ") { "${it.key.partition}:${it.value}" }

    private suspend fun withAdmin(use: suspend (KafkaAdmin) -> Unit) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            use(admin)
        } finally {
            admin.close()
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
}
