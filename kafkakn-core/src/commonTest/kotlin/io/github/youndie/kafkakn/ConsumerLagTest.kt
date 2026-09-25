package io.github.youndie.kafkakn

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
 * [B-53](../../../../../../../docs/backlog/B-53-consumer-lag-in-metrics.md): the consumer's lag, read from
 * each arm's own client.
 *
 * Lag here is the end of the log minus the position: what this consumer has not read yet. That is the
 * Java client's `currentLag`, and librdkafka's `consumer_lag_stored` (its `consumer_lag` is measured from
 * the committed offset instead, per `STATISTICS.md`).
 *
 * The two clients measure at different moments: on each fetch, and once per statistics interval. So the
 * lag is **frozen** before it is read. The member reads part of a 2000-record partition, pauses it (B-52),
 * commits its position and keeps polling past two statistics intervals. The expected lag is then exact,
 * on both arms, and `kafka-consumer-groups.sh` (which measures from the commit, now equal to the position)
 * must say the same, in `ci/b-53/run.sh`.
 */
class ConsumerLagTest {
    @Test
    fun a_frozen_lag_is_the_end_minus_the_position_and_zero_once_everything_is_read() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-lag-$armName-${randomSuffix()}"
                val partition = TopicPartition(topic, 0)
                create(topic)
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to topic,
                            "auto.offset.reset" to "earliest",
                        ),
                    )
                try {
                    consumer.assign(listOf(partition))
                    var read = 0L
                    val until = TimeSource.Monotonic.markNow() + READ_FOR
                    while (read < PART && until.hasNotPassedNow()) read += consumer.poll(POLL).size
                    consumer.pause(listOf(partition))
                    val position = consumer.position(partition)
                    // Before any commit, first: a lag measured from the committed offset (librdkafka's
                    // consumer_lag) would differ from one measured from the position here, and only here.
                    settle(consumer)
                    val uncommitted = consumer.metrics().lag[partition]
                    assertEquals(RECORDS - position, uncommitted, "the lag at $position, nothing committed yet")
                    consumer.commit(mapOf(partition to position))
                    settle(consumer)
                    val lag = consumer.metrics().lag[partition]
                    assertEquals(RECORDS - position, lag, "the lag with the partition paused at $position")
                    recordArmFact("lag.topic", topic)
                    recordArmFact("lag.position", position.toString())
                    recordArmFact("lag.frozen", lag.toString())
                    recordArmFact("lag.uncommitted", uncommitted.toString())

                    consumer.resume(listOf(partition))
                    val rest = TimeSource.Monotonic.markNow() + READ_FOR
                    while (read < RECORDS && rest.hasNotPassedNow()) read += consumer.poll(POLL).size
                    assertEquals(RECORDS, read, "records read")
                    settle(consumer)
                    val drained = consumer.metrics().lag[partition]
                    assertEquals(0L, drained, "the lag once everything is read")
                    recordObservation("lag.drained", drained.toString())
                    assertTrue(position >= PART, "the lag was frozen before a first batch was read")
                } finally {
                    consumer.close()
                }
            }
        }

    /**
     * Two statistics intervals of polling that returns nothing new: the native arm's figures come from
     * statistics delivered inside `poll`, once a second.
     */
    private suspend fun settle(consumer: KafkaConsumer) {
        val until = TimeSource.Monotonic.markNow() + SETTLE
        while (until.hasNotPassedNow()) consumer.poll(POLL)
    }

    private suspend fun create(topic: String) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, partitions = 1, replicationFactor = 1)))
        } finally {
            admin.close()
        }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            repeat(
                RECORDS.toInt(),
            ) { index -> producer.send(ProducerRecord(topic, "$index".encodeToByteArray(), partition = 0)) }
        } finally {
            producer.close()
        }
    }

    private companion object {
        const val RECORDS = 2000L
        const val PART = 500L
        val POLL = 200.milliseconds
        val READ_FOR = 30.seconds
        val SETTLE = 3.seconds
    }
}
