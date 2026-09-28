package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-101](../../../../../../../docs/backlog/B-101-describe-right-after-create-is-an-unknown-topic.md): a MEASUREMENT.
 * `AdminPartitionsTest` described a topic it had just created and grown, and was told the topic is unknown. Which
 * step was it, and how often?
 *
 * `KAFKAKN_B101_ROUNDS` rounds, each on a new topic: `createTopics`, then `describeTopics` at once; then
 * `createPartitions` to two, and `describeTopics` at once. Each step records what the first description said and how
 * long until the broker's description agreed. Nothing is asserted: each round's line is recorded.
 */
class CreateThenDescribeTest {
    @Test
    fun a_topic_described_at_once_after_it_was_created_and_grown() =
        runTest(timeout = 30.minutes) {
            val rounds = testEnv("KAFKAKN_B101_ROUNDS")?.toIntOrNull()
            if (rounds == null) {
                recordArmFact("b101", "not asked")
                return@runTest
            }
            withContext(Dispatchers.Default) {
                val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
                try {
                    var createdUnknown = 0
                    var grownShort = 0
                    var grownUnknown = 0
                    for (round in 0 until rounds) {
                        val topic = "kafkakn-b101-$armName-${randomSuffix()}"
                        admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                        val afterCreate = describe(admin, topic)
                        if (afterCreate == UNKNOWN) createdUnknown++
                        val createdMs = millisUntil { describe(admin, topic) == "1" }
                        admin.createPartitions(topic, 2)
                        val afterGrow = describe(admin, topic)
                        if (afterGrow == UNKNOWN) grownUnknown++
                        if (afterGrow == "1") grownShort++
                        val grownMs = millisUntil { describe(admin, topic) == "2" }
                        recordArmFact(
                            "b101.$round",
                            "created=$afterCreate visible.ms=$createdMs grown=$afterGrow visible.ms=$grownMs",
                        )
                        admin.deleteTopics(listOf(topic))
                    }
                    recordArmFact(
                        "b101",
                        "rounds=$rounds created.unknown=$createdUnknown grown.unknown=$grownUnknown grown.short=$grownShort",
                    )
                } finally {
                    admin.close()
                }
            }
        }

    /** The number of partitions the broker describes, or what it said instead. */
    private suspend fun describe(
        admin: KafkaAdmin,
        topic: String,
    ): String =
        try {
            admin
                .describeTopics(listOf(topic))
                .getValue(topic)
                .size
                .toString()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (thrown: KafkaAdminException) {
            // librdkafka's words, then the Java client's for the same error code, UNKNOWN_TOPIC_OR_PARTITION (3).
            val said = thrown.message.orEmpty()
            if ("nknown topic" in said || "does not host this topic" in said) UNKNOWN else "threw $said"
        }

    private suspend fun millisUntil(agreed: suspend () -> Boolean): Long {
        val started = TimeSource.Monotonic.markNow()
        while (!agreed()) {
            if (started.elapsedNow() > GIVE_UP) return -1
            delay(RETRY)
        }
        return started.elapsedNow().inWholeMilliseconds
    }

    private companion object {
        const val UNKNOWN = "unknown"
        val RETRY = 5.milliseconds
        val GIVE_UP = 10.seconds
    }
}
