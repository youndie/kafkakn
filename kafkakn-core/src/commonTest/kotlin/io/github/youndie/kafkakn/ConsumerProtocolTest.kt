package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-57](../../../../../../../docs/backlog/B-57-the-kip-848-consumer-protocol.md): the KIP-848 group protocol,
 * `group.protocol=consumer`, on each arm. The mixed group is `CooperativeTest`'s member under this protocol,
 * run by `ci/b-57/run.sh`, which also reads the group's type from the broker's own tool.
 */
class ConsumerProtocolTest {
    @Test
    fun a_lone_member_under_the_consumer_protocol_reads_commits_and_is_told_of_its_partitions() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val group = "kafkakn-848-$armName-${randomSuffix()}"
                val heard = mutableListOf<String>()
                val consumer = kafkaConsumer(config(group))
                val partition = TopicPartition(consumeTopic, 0)
                try {
                    consumer.subscribe(
                        listOf(consumeTopic),
                        object : RebalanceListener {
                            override fun onAssigned(partitions: List<TopicPartition>) {
                                heard += "+${partitions.map { it.partition }}"
                            }

                            override fun onRevoked(
                                partitions: List<TopicPartition>,
                                scope: RebalanceScope,
                            ) {
                                heard += "-${partitions.map { it.partition }}"
                                // B-50's promise: a commit from the revocation lands before the partition moves.
                                scope.commit(mapOf(partition to REVOKED_AT))
                            }

                            override fun onLost(partitions: List<TopicPartition>) {
                                heard += "!${partitions.map { it.partition }}"
                            }
                        },
                    )
                    val read = mutableListOf<Long>()
                    val until = TimeSource.Monotonic.markNow() + READ_WITHIN
                    while (read.size < CONSUME_COUNT && until.hasNotPassedNow()) {
                        read += consumer.poll(POLL).map { it.offset }
                    }
                    assertEquals((0L until CONSUME_COUNT).toList(), read, "every record once, in order")
                    // B-48: explicit offsets, read back.
                    consumer.commit(mapOf(partition to COMMITTED_AT))
                    assertEquals(mapOf(partition to COMMITTED_AT), consumer.committed(listOf(partition)))
                    recordObservation("848.lone.read", "${read.size} ${read.first()}..${read.last()}")
                    recordObservation("848.lone.committed", COMMITTED_AT.toString())
                } finally {
                    consumer.close()
                }
                // The revocation's commit, made on close, as the broker's group now holds it.
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    val after = admin.listConsumerGroupOffsets(group)
                    recordObservation("848.lone.heard", heard.joinToString(" "))
                    recordObservation("848.lone.after.close", after.values.joinToString())
                    recordArmFact("848.lone.group", group)
                    assertEquals(listOf("+[0]", "-[0]"), heard, "the listener, under the new protocol")
                    assertEquals(mapOf(partition to REVOKED_AT), after, "the commit made in onRevoked, on close")
                } finally {
                    admin.close()
                }
            }
        }

    /**
     * Keys the classic protocol reads and the consumer protocol moves to the broker. What each arm does with
     * them is recorded and compared: a key accepted and dropped looks identical to one that worked.
     */
    @Test
    fun keys_the_consumer_protocol_moves_to_the_broker() =
        runTest(timeout = 1.minutes) {
            withContext(Dispatchers.Default) {
                for ((key, value) in listOf(
                    "partition.assignment.strategy" to "range",
                    "session.timeout.ms" to "30000",
                    "heartbeat.interval.ms" to "3000",
                )) {
                    val said = outcome { kafkaConsumer(config("kafkakn-848-keys", key to value)).close() }
                    recordArmFact("848.key.$key.said", said)
                    recordObservation("848.key.$key", said.substringBefore(":"))
                    assertEquals("threw IllegalArgumentException", said.substringBefore(":"), key)
                }
                // The same key under the classic protocol is still the client's to judge: accepted by both.
                val classic =
                    outcome {
                        kafkaConsumer(
                            ConsumerConfig(
                                "bootstrap.servers" to bootstrap,
                                "group.id" to "g",
                                "session.timeout.ms" to "30000",
                            ),
                        ).close()
                    }
                assertEquals("done", classic, "session.timeout.ms under the classic protocol")
            }
        }

    private fun config(
        group: String,
        vararg extra: Pair<String, String>,
    ) = ConsumerConfig(
        mapOf(
            "bootstrap.servers" to bootstrap,
            "group.id" to group,
            "group.protocol" to "consumer",
            "auto.offset.reset" to "earliest",
        ) + extra,
    )

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

    private companion object {
        const val COMMITTED_AT = 12L
        const val REVOKED_AT = 17L
        val POLL = 200.milliseconds
        val READ_WITHIN = 30.seconds
    }
}
