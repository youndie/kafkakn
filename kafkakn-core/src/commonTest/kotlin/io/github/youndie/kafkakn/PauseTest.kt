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
 * [B-52](../../../../../../../docs/backlog/B-52-pause-and-resume.md): pause a partition without leaving
 * the group, and resume it where it stopped.
 *
 * A topic of its own: 2000 records on partition 0, more than one batch (500), so that when it is paused
 * the client may already hold records it fetched for it. That is the item's trap. Partition 1 is written
 * to only while partition 0 is paused, so "the others still return records" is about the pause, not
 * about records that happened to arrive before it.
 * `max.poll.interval.ms` is 6 s (librdkafka wants `session.timeout.ms` no higher, so that is 6 s too), and
 * the member polls for longer than that with partition 0 paused. It must still be in its group afterwards,
 * and prove it with a commit, which a removed member cannot make. `ci/b-52/run.sh` counts partition 0
 * against the broker's end offset.
 */
class PauseTest {
    @Test
    fun a_paused_partition_returns_nothing_the_member_stays_and_resume_repeats_and_skips_nothing() =
        runTest(timeout = 3.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-pause-$armName-${randomSuffix()}"
                val big = TopicPartition(topic, 0)
                val small = TopicPartition(topic, 1)
                fill(topic)
                val consumer =
                    kafkaConsumer(
                        ConsumerConfig(
                            "bootstrap.servers" to bootstrap,
                            "group.id" to topic,
                            "auto.offset.reset" to "earliest",
                            "max.poll.interval.ms" to "6000",
                            "session.timeout.ms" to "6000",
                        ),
                    )
                val seen = mutableListOf<Long>()
                var fromBigWhilePaused = 0
                var fromSmallWhilePaused = 0
                try {
                    consumer.subscribe(listOf(topic))
                    // Until the first records of partition 0 arrive: it is paused with more of it fetched,
                    // or being fetched, than was returned.
                    val until = TimeSource.Monotonic.markNow() + READ_FOR
                    while (seen.isEmpty() && until.hasNotPassedNow()) {
                        consumer.poll(POLL).forEach { if (it.partition == 0) seen += it.offset }
                    }
                    check(seen.isNotEmpty()) { "partition 0 returned nothing in $READ_FOR" }
                    consumer.pause(listOf(big))
                    assertEquals(listOf(big), consumer.paused(), "paused, as the consumer reports it")
                    produce(topic, partition = 1, count = SMALL)
                    val pausedAt = TimeSource.Monotonic.markNow()
                    while (pausedAt.elapsedNow() < PAUSED_FOR) {
                        for (record in consumer.poll(POLL)) {
                            if (record.partition == 0) fromBigWhilePaused++ else fromSmallWhilePaused++
                        }
                    }
                    // Still a member: the partitions are still held, and a commit is accepted.
                    assertEquals(
                        listOf(big, small),
                        consumer.assignment(),
                        "held after polling past max.poll.interval.ms",
                    )
                    consumer.commit(mapOf(small to 1L))
                    consumer.resume(listOf(big))
                    assertEquals(emptyList(), consumer.paused(), "nothing paused after resume")
                    val resumed = TimeSource.Monotonic.markNow() + READ_FOR
                    while (seen.size < BIG && resumed.hasNotPassedNow()) {
                        consumer.poll(POLL).forEach { if (it.partition == 0) seen += it.offset }
                    }
                } finally {
                    consumer.close()
                }
                recordArmFact("pause.topic", topic)
                recordArmFact("pause.seen", seen.size.toString())
                recordArmFact("pause.distinct", seen.distinct().size.toString())
                recordObservation("pause.returned.while.paused", fromBigWhilePaused.toString())
                assertEquals(0, fromBigWhilePaused, "records of the paused partition returned while it was paused")
                assertTrue(fromSmallWhilePaused > 0, "the partition that was not paused returned nothing")
                assertEquals((0L until BIG).toList(), seen, "partition 0, before the pause and after resume")
            }
        }

    /**
     * A seek does not undo a pause. On native it matters: a seek with `assign` re-assigns every partition,
     * and librdkafka starts an assignment unpaused, so the pause has to be put back.
     */
    @Test
    fun a_seek_does_not_undo_a_pause() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                val partition = TopicPartition(consumeTopic, 0)
                val consumer = kafkaConsumer(ConsumerConfig("bootstrap.servers" to bootstrap))
                try {
                    consumer.assign(listOf(partition))
                    consumer.pause(listOf(partition))
                    consumer.seek(partition, SeekTo.Beginning)
                    var whilePaused = 0
                    val pausedAt = TimeSource.Monotonic.markNow()
                    while (pausedAt.elapsedNow() < QUIET) whilePaused += consumer.poll(POLL).size
                    assertEquals(listOf(partition), consumer.paused(), "still paused after the seek")
                    consumer.resume(listOf(partition))
                    var afterResume = 0
                    val until = TimeSource.Monotonic.markNow() + READ_FOR
                    while (afterResume < CONSUME_COUNT &&
                        until.hasNotPassedNow()
                    ) {
                        afterResume += consumer.poll(POLL).size
                    }
                    assertEquals(0, whilePaused, "records returned by a paused partition after a seek")
                    assertEquals(CONSUME_COUNT, afterResume, "records after resume, from the beginning the seek named")
                    recordObservation("pause.after.seek", "$whilePaused/$afterResume")
                } finally {
                    consumer.close()
                }
            }
        }

    private suspend fun fill(topic: String) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, partitions = 2, replicationFactor = 1)))
        } finally {
            admin.close()
        }
        produce(topic, partition = 0, count = BIG)
    }

    private suspend fun produce(
        topic: String,
        partition: Int,
        count: Long,
    ) {
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
        try {
            repeat(count.toInt()) { index ->
                producer.send(ProducerRecord(topic, "$partition:$index".encodeToByteArray(), partition = partition))
            }
        } finally {
            producer.close()
        }
    }

    private companion object {
        const val BIG = 2000L
        const val SMALL = 50L
        val POLL = 200.milliseconds
        val READ_FOR = 30.seconds
        val PAUSED_FOR = 10.seconds
        val QUIET = 3.seconds
    }
}
