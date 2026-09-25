package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * [B-61](../../../../../../../docs/backlog/B-61-topic-configs.md): a topic's configuration, described and
 * changed incrementally through the admin client. `ci/b-61/run.sh` holds each arm's description against
 * `kafka-configs.sh --describe --all` on that arm's topic.
 */
class AdminConfigsTest {
    @Test
    fun a_topics_configuration_is_described_changed_incrementally_and_returned_to_its_default() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-configs-$armName-${randomSuffix()}"
                withAdmin { admin ->
                    admin.createTopics(listOf(NewTopic(topic, 1, 1, mapOf("retention.ms" to RETENTION))))
                    val created = watched(admin, topic)
                    assertEquals(
                        mapOf(
                            "retention.ms" to "$RETENTION/TOPIC",
                            "max.message.bytes" to "1048588/DEFAULT",
                            "cleanup.policy" to "delete/DEFAULT",
                        ),
                        created,
                        "as created",
                    )

                    // Incremental: one key set, the one set at creation left alone.
                    admin.alterTopicConfigs(topic, set = mapOf("max.message.bytes" to MAX_MESSAGE))
                    val altered = watchedOnce(admin, topic, "set") { it["max.message.bytes"] == "$MAX_MESSAGE/TOPIC" }
                    assertEquals("$MAX_MESSAGE/TOPIC", altered["max.message.bytes"], "the key set")
                    assertEquals("$RETENTION/TOPIC", altered["retention.ms"], "a key not named is left alone")
                    recordArmFact("configs.topic", topic)
                    recordArmFact(
                        "configs.altered",
                        rendered(admin.describeTopicConfigs(listOf(topic)).getValue(topic)),
                    )

                    // Deleted: back to what the topic would have without it.
                    admin.alterTopicConfigs(topic, delete = listOf("retention.ms"))
                    val deleted = watchedOnce(admin, topic, "delete") { it["retention.ms"] != "$RETENTION/TOPIC" }
                    assertEquals("604800000/DEFAULT", deleted["retention.ms"], "a deleted key returns to its default")
                    assertEquals("$MAX_MESSAGE/TOPIC", deleted["max.message.bytes"], "and the other stays")

                    recordObservation("configs.created", created.render())
                    recordObservation("configs.altered", altered.render())
                    recordObservation("configs.deleted", deleted.render())
                    recordArmFact(
                        "configs.after.delete",
                        rendered(admin.describeTopicConfigs(listOf(topic)).getValue(topic)),
                    )
                }
            }
        }

    @Test
    fun what_the_broker_refuses_is_refused_alike_on_both_arms_and_changes_nothing() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val topic = "kafkakn-configs-refused-$armName-${randomSuffix()}"
                withAdmin { admin ->
                    admin.createTopics(listOf(NewTopic(topic, 1, 1)))
                    val refusals =
                        listOf(
                            "unknown.key" to
                                outcome { admin.alterTopicConfigs(topic, set = mapOf("kafkakn.no.such.key" to "1")) },
                            // A known key with a value the broker cannot read, next to a valid one: all or nothing.
                            "bad.value" to
                                outcome {
                                    admin.alterTopicConfigs(
                                        topic,
                                        set =
                                            mapOf(
                                                "retention.ms" to "soon",
                                                "max.message.bytes" to MAX_MESSAGE,
                                            ),
                                    )
                                },
                            "missing.topic" to
                                outcome {
                                    admin.describeTopicConfigs(
                                        listOf("kafkakn-no-such-topic-${randomSuffix()}"),
                                    )
                                },
                        )
                    refusals.forEach { (what, said) ->
                        recordArmFact("configs.refused.$what.said", said)
                        // A missing topic is each client's own failure (recorded, not promised): compared as refused.
                        val compared =
                            if (what == "missing.topic" &&
                                said.startsWith("threw")
                            ) {
                                "refused"
                            } else {
                                said.substringBefore(":")
                            }
                        recordObservation("configs.refused.$what", compared)
                    }
                    // A sentinel change, waited for: changes reach the broker in order, so once it is visible any
                    // half of a refused call that had been applied would be visible too. Read at once, a wrongly
                    // applied change still on its way would pass for one refused.
                    admin.alterTopicConfigs(topic, set = mapOf("segment.jitter.ms" to "1"))
                    val started = TimeSource.Monotonic.markNow()
                    while (admin
                            .describeTopicConfigs(
                                listOf(topic),
                            ).getValue(topic)
                            .getValue("segment.jitter.ms")
                            .value !=
                        "1"
                    ) {
                        check(
                            started.elapsedNow() < VISIBLE_WITHIN,
                        ) { "the sentinel was not visible after $VISIBLE_WITHIN" }
                        delay(RETRY)
                    }
                    val after = watched(admin, topic)
                    recordObservation("configs.refused.after", after.render())
                    assertEquals(
                        "1048588/DEFAULT",
                        after["max.message.bytes"],
                        "the valid half of a refused call is not applied",
                    )
                    assertEquals(
                        listOf("unknown.key", "bad.value").map { it to "threw IllegalArgumentException" },
                        refusals.take(2).map { (what, said) -> what to said.substringBefore(":") },
                    )
                }
            }
        }

    /**
     * [watched], once it shows [changed]. A change is accepted by the controller and reaches the broker's view
     * of the topic a moment later, so a describe right after the call can still show the old value: seen on
     * the native arm once, and a race on either. How long it took is recorded; a change never seen fails here.
     */
    private suspend fun watchedOnce(
        admin: KafkaAdmin,
        topic: String,
        what: String,
        changed: (Map<String, String>) -> Boolean,
    ): Map<String, String> {
        val started = TimeSource.Monotonic.markNow()
        while (true) {
            val seen = watched(admin, topic)
            if (changed(seen)) {
                recordArmFact("configs.$what.visible.after.ms", started.elapsedNow().inWholeMilliseconds.toString())
                return seen
            }
            check(started.elapsedNow() < VISIBLE_WITHIN) { "the $what was not visible after $VISIBLE_WITHIN: $seen" }
            delay(RETRY)
        }
    }

    /** The three keys this test changes, as `value/source`. */
    private suspend fun watched(
        admin: KafkaAdmin,
        topic: String,
    ): Map<String, String> {
        val all = admin.describeTopicConfigs(listOf(topic)).getValue(topic)
        return WATCHED.associateWith { key -> all.getValue(key).let { "${it.value}/${it.source}" } }
    }

    /** Every key, `key=value/source`, for the runner to hold against `kafka-configs.sh --describe --all`. */
    private fun rendered(entries: Map<String, TopicConfigEntry>): String =
        entries.entries.joinToString(";") { (key, entry) -> "$key=${entry.value}/${entry.source}" }

    private fun Map<String, String>.render(): String = entries.joinToString(" ") { "${it.key}=${it.value}" }

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

    private companion object {
        const val RETENTION = "86400000"
        const val MAX_MESSAGE = "2000000"
        val WATCHED = listOf("retention.ms", "max.message.bytes", "cleanup.policy")
        val VISIBLE_WITHIN = 10.seconds
        val RETRY = 100.milliseconds
    }
}
