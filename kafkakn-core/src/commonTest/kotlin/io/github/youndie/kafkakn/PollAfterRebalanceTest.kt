package io.github.youndie.kafkakn

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-68](../../../../../../../docs/backlog/B-68-native-poll-returns-records-of-a-revoked-partition.md): a `poll`
 * returns no record of a partition its listener gave up during that same `poll`.
 *
 * Member A reads two partitions with a deep backlog, so the client holds many fetched records it has not yet
 * returned. Member B joins, and the group rebalances eagerly: A gives up both partitions and is given one back.
 * The rebalance reaches A inside a `poll`. Every record A is handed afterwards must be of a partition A holds,
 * and none may come from before A gave the partitions up, as the Java client guarantees.
 *
 * **It did not reproduce B-68's defect**, fast or slow: librdkafka serves a rebalance ahead of the records
 * queued before it, so a stray needs the rebalance to arrive while a `poll` is part-way through collecting,
 * which a member here almost never is. It stays as the guard for the ordinary rebalance. The defect itself
 * is reproduced, and its fix held, by `ci/b-68/run.sh`, which freezes a member again and again.
 */
class PollAfterRebalanceTest {
    @Test
    fun a_poll_returns_no_record_of_a_partition_given_up_during_it() =
        runTest(timeout = 4.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-poll-rebalance-$armName-${randomSuffix()}"
                val group = topic
                fill(topic)
                val held = mutableSetOf<Int>()
                var revocations = 0
                // Nobody commits here, so a partition given back after a revocation is read again from offset 0.
                // The first record of it that is not offset 0 was fetched before the revocation.
                val restarting = mutableSetOf<Int>()
                // A record of a partition A does not hold, or one fetched before A gave its partition up.
                val strays = mutableListOf<String>()
                val a = member(group)
                val bAssigned = CompletableDeferred<Unit>()
                val done = CompletableDeferred<Unit>()
                try {
                    a.subscribe(
                        listOf(topic),
                        object : RebalanceListener {
                            override fun onAssigned(partitions: List<TopicPartition>) {
                                held += partitions.map { it.partition }
                            }

                            override fun onRevoked(
                                partitions: List<TopicPartition>,
                                scope: RebalanceScope,
                            ) {
                                revocations++
                                held -= partitions.map { it.partition }.toSet()
                                restarting += partitions.map { it.partition }
                            }
                        },
                    )
                    // A reads a little of its backlog: enough to be fetching, far from the end.
                    var read = 0
                    val until = TimeSource.Monotonic.markNow() + WITHIN
                    while (read < BEFORE_B && until.hasNotPassedNow()) read += a.poll(POLL).size
                    check(read >= BEFORE_B) { "A read $read records before B joined" }
                    check(revocations == 0) { "A was revoked before B joined" }
                    coroutineScope {
                        launch {
                            val b = member(group)
                            try {
                                b.subscribe(listOf(topic))
                                val bUntil = TimeSource.Monotonic.markNow() + WITHIN
                                while (b.assignment().isEmpty() && bUntil.hasNotPassedNow()) b.poll(POLL)
                                bAssigned.complete(Unit)
                                done.await()
                            } finally {
                                b.close()
                            }
                        }
                        val aUntil = TimeSource.Monotonic.markNow() + WITHIN
                        // Until A has been through the rebalance and read on after it.
                        var afterRebalance = 0
                        while (aUntil.hasNotPassedNow() && (!bAssigned.isCompleted || afterRebalance < AFTER)) {
                            // Slowly, so the client keeps a deep queue of fetched records when the rebalance reaches
                            // it: read at full speed, A emptied its backlog long before B's join arrived, and the
                            // rebalance found nothing queued ahead of it.
                            delay(SLOW)
                            val records = a.poll(POLL)
                            for (record in records) {
                                if (record.partition !in held) strays += "${record.partition}:${record.offset} not held"
                                if (restarting.remove(record.partition) && record.offset != 0L) {
                                    strays += "${record.partition}:${record.offset} fetched before the revocation"
                                }
                            }
                            if (revocations > 0) afterRebalance += records.size
                        }
                        done.complete(Unit)
                    }
                    check(revocations > 0) { "A never went through the rebalance" }
                    recordArmFact("poll.rebalance.strays", strays.joinToString(" ").ifEmpty { "none" })
                    recordObservation("poll.rebalance.strays", strays.size.toString())
                    assertEquals(emptyList(), strays.take(5), "records handed over after the listener gave them up")
                } finally {
                    a.close()
                }
            }
        }

    private fun member(group: String) =
        kafkaConsumer(
            ConsumerConfig(
                "bootstrap.servers" to bootstrap,
                "group.id" to group,
                "auto.offset.reset" to "earliest",
                "partition.assignment.strategy" to "range",
            ),
        )

    private suspend fun fill(topic: String) {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            admin.createTopics(listOf(NewTopic(topic, 2, 1)))
        } finally {
            admin.close()
        }
        val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "linger.ms" to "5"))
        try {
            // In parallel, a batch at a time: one send awaited after another took minutes for this many.
            for (batch in 0 until PER_PARTITION / BATCH) {
                coroutineScope {
                    for (index in batch * BATCH until (batch + 1) * BATCH) {
                        for (partition in 0..1) {
                            launch {
                                producer.send(
                                    ProducerRecord(
                                        topic,
                                        ByteArray(RECORD_BYTES) { index.toByte() },
                                        partition = partition,
                                    ),
                                )
                            }
                        }
                    }
                }
            }
        } finally {
            producer.close()
        }
    }

    private companion object {
        const val PER_PARTITION = 10_000
        const val RECORD_BYTES = 1_000
        const val BATCH = 1_000
        const val BEFORE_B = 200
        const val AFTER = 1_000
        val POLL = 200.milliseconds
        val SLOW = 300.milliseconds
        val WITHIN = 90.seconds
    }
}
