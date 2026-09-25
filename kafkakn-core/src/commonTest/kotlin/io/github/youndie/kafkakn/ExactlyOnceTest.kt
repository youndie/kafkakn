package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

/**
 * [B-38](../../../../../../../docs/backlog/B-38-exactly-once-read-process-write.md): read, process, write,
 * and commit the input's progress inside the output's transaction — with the processor stopped at random
 * points and started again.
 *
 * Each output record names the input record it came from, `stamp:partition:offset`, so the oracle in
 * `ci/b-38/run.sh` can say of every input record how many times it reached the output: under
 * `read_committed` exactly once is the claim; under `read_uncommitted` the aborted attempts must show,
 * or the run never produced the failures the green is about.
 *
 * A "stop" leaves the transaction open — after the output was sent, or after the offsets were added —
 * and walks away without committing. The next instance, with the same `transactional.id`, fences the
 * old one and its open transaction is aborted.
 */
class ExactlyOnceTest {
    @Test
    fun every_input_record_reaches_the_output_exactly_once_across_stops() =
        runTest(timeout = 4.minutes) {
            val input = testEnv("KAFKAKN_EOS_INPUT") ?: consumeTopic
            val output = testEnv("KAFKAKN_EOS_OUTPUT") ?: testTopic
            val expected = testEnv("KAFKAKN_EOS_COUNT")?.toInt() ?: CONSUME_COUNT
            val seed = testEnv("KAFKAKN_EOS_SEED")?.toInt() ?: Random.nextInt()
            val suffix = randomSuffix()
            val stamp = "eos-$armName-$suffix"
            val run = Run(input, output, stamp, "kafkakn-eos-$armName-$suffix", expected, Random(seed))
            withContext(Dispatchers.Default) {
                var instance = 0
                while (!run.done()) {
                    check(
                        instance < MAX_INSTANCES,
                    ) { "no end after $instance instances: ${run.committed.size} of $expected" }
                    run.instance(stopAt = if (instance < STOPS) run.stopPoint() else null)
                    instance++
                }
            }
            recordArmFact("eos.stamp", stamp)
            recordArmFact("eos.expected", expected.toString())
            recordArmFact("eos.seed", seed.toString())
            recordArmFact("eos.stops", run.stops.joinToString(";"))
            assertTrue(
                run.stops.size == STOPS,
                "the processor was stopped ${run.stops.size} times, not $STOPS: ${run.stops}",
            )
        }

    private enum class Stop { AFTER_OUTPUT, AFTER_OFFSETS }

    private inner class Run(
        val input: String,
        val output: String,
        val stamp: String,
        val identity: String,
        val expected: Int,
        val random: Random,
    ) {
        /** Input records whose output and progress were committed together, by this run's own account. */
        val committed = mutableSetOf<String>()
        val stops = mutableListOf<String>()

        fun done() = committed.size >= expected

        /**
         * Where the next instance stops, drawn from the seed: after its output or after its offsets, in the
         * first batch it processes - whose size depends on when it started against the trickling input.
         */
        fun stopPoint() = Stop.entries[random.nextInt(Stop.entries.size)]

        /**
         * One instance of the processor: a consumer and a transactional producer, the same group and the
         * same `transactional.id` as every instance before it.
         */
        suspend fun instance(stopAt: Stop?) {
            val consumer =
                kafkaConsumer(
                    ConsumerConfig(
                        "bootstrap.servers" to bootstrap,
                        "group.id" to identity,
                        "auto.offset.reset" to "earliest",
                    ),
                )
            val producer =
                kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "transactional.id" to identity))
            try {
                producer.initTransactions()
                consumer.subscribe(listOf(input))
                var batches = 0
                val quietUntil = TimeSource.Monotonic.markNow() + QUIET
                while (!done()) {
                    val batch = consumer.poll(POLL)
                    if (batch.isEmpty()) {
                        val waiting = quietUntil.hasNotPassedNow() || batches > 0
                        check(waiting) { "the input was silent: ${committed.size} of $expected" }
                        continue
                    }
                    batches++
                    producer.beginTransaction()
                    for (record in batch) {
                        producer.send(
                            ProducerRecord(output, "$stamp:${record.partition}:${record.offset}".encodeToByteArray()),
                        )
                    }
                    if (stopAt == Stop.AFTER_OUTPUT) {
                        stops += "${batch.size} records, after the output"
                        return
                    }
                    // One past the last record processed, per partition: where the next instance resumes.
                    val next =
                        batch
                            .groupBy { TopicPartition(it.topic, it.partition) }
                            .mapValues { (_, records) -> records.maxOf { it.offset } + 1 }
                    producer.sendOffsetsToTransaction(next, consumer.groupMetadata())
                    if (stopAt == Stop.AFTER_OFFSETS) {
                        stops += "${batch.size} records, after the offsets"
                        return
                    }
                    producer.commitTransaction()
                    committed += batch.map { "${it.partition}:${it.offset}" }
                }
            } finally {
                // A stop walks away with the transaction open; the next instance's initTransactions
                // fences this producer, and the coordinator aborts what it left.
                consumer.close()
                producer.close()
            }
        }
    }

    private companion object {
        const val STOPS = 3
        const val MAX_INSTANCES = 12
        val POLL = 300.milliseconds
        val QUIET = 30_000.milliseconds
    }
}
