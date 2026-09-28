package io.github.youndie.kafkakn

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [B-100](../../../../../../../docs/backlog/B-100-poll-drops-records-given-up-mid-drain.md): the platform seam, since
 * only the native `drain` collects across several `rd_kafka_consumer_poll` calls, and a rebalance callback can run in
 * a later one. The records it had already collected for the partitions that callback took must not reach the caller,
 * as the Java client returns no fetched record of a revoked partition (consumer contract §2a).
 *
 * The rebalance itself arriving mid-drain is rare and not reproducible on demand: the suite saw it once, 486 records
 * of partition 0, on a GitHub-hosted runner. `PollAfterRebalanceTest` stays the integration guard on both arms; this
 * test is what fails when the filter is taken out.
 */
class NativeDrainGivenUpTest {
    @Test
    fun records_collected_for_a_partition_a_callback_took_are_dropped_and_the_rest_kept() {
        val collected =
            mutableListOf(
                record("orders", 0, 1500),
                record("orders", 1, 700),
                record("orders", 0, 1501),
                record("audit", 0, 9),
                record("orders", 1, 701),
            )

        val dropped = dropGivenUp(collected, setOf(TopicPartition("orders", 0)))

        assertEquals(2, dropped, "how many were dropped, which B-68's counter reports")
        assertEquals(
            listOf("orders-1@700", "audit-0@9", "orders-1@701"),
            collected.map { "${it.topic}-${it.partition}@${it.offset}" },
            "the partitions still held keep their records, in the order they were fetched",
        )
    }

    @Test
    fun nothing_is_dropped_when_the_callback_took_a_partition_with_no_record_collected() {
        val collected = mutableListOf(record("orders", 1, 700))

        assertEquals(0, dropGivenUp(collected, setOf(TopicPartition("orders", 0))))
        assertEquals(1, collected.size)
    }

    private fun record(
        topic: String,
        partition: Int,
        offset: Long,
    ) = ConsumerRecord(topic, partition, offset, 0, null, null, emptyList())
}
