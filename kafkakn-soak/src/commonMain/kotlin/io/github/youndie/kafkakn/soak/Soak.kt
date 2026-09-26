package io.github.youndie.kafkakn.soak

import io.github.youndie.kafkakn.ConsumerConfig
import io.github.youndie.kafkakn.ProducerConfig
import io.github.youndie.kafkakn.ProducerRecord
import io.github.youndie.kafkakn.TopicPartition
import io.github.youndie.kafkakn.kafkaConsumer
import io.github.youndie.kafkakn.kafkaProducer
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * One instance of the soak service ([B-70](../../../../../../../../docs/backlog/B-70-kafkakn-soak.md)): read the
 * input in a group, write one output record per input record, and commit the input's progress inside the
 * output's transaction (B-38's loop, as a program).
 *
 * **Crash-only.** Nothing is caught here: any failure ends the instance, and `ci/b-70/run.sh` starts it again.
 * A fenced producer, a lost membership, an aborted transaction all leave by the same door, so each recovery is
 * the library's and the broker's, never a guess of this service about which errors are safe to continue past.
 *
 * The output record names its input, `partition:offset:value`, so the runner can count each input record in
 * the output. The `transactional.id` is the instance's slot, the same across its restarts: a restarted
 * instance fences the one it replaces, and a frozen one that wakes finds itself fenced.
 */
internal suspend fun runSoak(env: (String) -> String?) {
    val bootstrap = env("SOAK_BOOTSTRAP") ?: "127.0.0.1:9092"
    val input = env("SOAK_INPUT") ?: error("SOAK_INPUT")
    val output = env("SOAK_OUTPUT") ?: error("SOAK_OUTPUT")
    val group = env("SOAK_GROUP") ?: error("SOAK_GROUP")
    val slot = env("SOAK_SLOT") ?: error("SOAK_SLOT")
    val consumer =
        kafkaConsumer(
            ConsumerConfig(
                "bootstrap.servers" to bootstrap,
                "group.id" to group,
                "auto.offset.reset" to "earliest",
                "session.timeout.ms" to (env("SOAK_SESSION_MS") ?: "10000"),
            ),
        )
    val producer =
        kafkaProducer(
            ProducerConfig(
                "bootstrap.servers" to bootstrap,
                "transactional.id" to "kafkakn-soak-$group-$slot",
                "acks" to "all",
            ),
        )
    producer.initTransactions()
    consumer.subscribe(listOf(input))
    say("$slot started")
    var records = 0L
    var transactions = 0L
    var lastSaid = TimeSource.Monotonic.markNow()
    while (true) {
        val batch = consumer.poll(POLL)
        if (batch.isNotEmpty()) {
            producer.beginTransaction()
            val next = mutableMapOf<TopicPartition, Long>()
            coroutineScope {
                batch
                    .map { record ->
                        next[TopicPartition(record.topic, record.partition)] = record.offset + 1
                        async {
                            producer.send(
                                ProducerRecord(
                                    output,
                                    "${record.partition}:${record.offset}:${record.value?.decodeToString()}"
                                        .encodeToByteArray(),
                                    key = slot.encodeToByteArray(),
                                    partition = 0,
                                ),
                            )
                        }
                    }.awaitAll()
            }
            producer.sendOffsetsToTransaction(next, consumer.groupMetadata())
            producer.commitTransaction()
            records += batch.size
            transactions++
        }
        if (lastSaid.elapsedNow() >= SAY_EVERY) {
            say(
                "$slot records=$records transactions=$transactions held=${consumer.assignment().map {
                    it.partition
                }.sorted()}",
            )
            lastSaid = TimeSource.Monotonic.markNow()
        }
    }
}

internal fun say(line: String) = println("soak $line")

private val POLL = 500.milliseconds
private val SAY_EVERY = 30.seconds
