package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/**
 * [B-103](../../../../../../../docs/backlog/B-103-an-unknown-topic-is-two-types.md): the broker's
 * `UNKNOWN_TOPIC_OR_PARTITION` (3) is one type on both arms, [UnknownTopicException], so common code can catch it.
 * Before, the JVM let the Java client's `UnknownTopicOrPartitionException` through and native threw a class that
 * existed only in `nativeMain`.
 */
class AdminUnknownTopicTest {
    @Test
    fun describing_a_topic_that_does_not_exist_is_an_unknown_topic_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val name = "kafkakn-b103-never-$armName-${randomSuffix()}"
                val refused = withAdmin { assertFailsWith<UnknownTopicException> { it.describeTopics(listOf(name)) } }
                assertTrue(name in refused.message.orEmpty(), "the message names the topic: ${refused.message}")
                recordObservation("admin.unknown.describe", refused::class.simpleName.orEmpty())
            }
        }

    @Test
    fun deleting_a_topic_that_does_not_exist_is_an_unknown_topic_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val name = "kafkakn-b103-never-$armName-${randomSuffix()}"
                val refused = withAdmin { assertFailsWith<UnknownTopicException> { it.deleteTopics(listOf(name)) } }
                recordObservation("admin.unknown.delete", refused::class.simpleName.orEmpty())
            }
        }

    private suspend fun <T> withAdmin(use: suspend (KafkaAdmin) -> T): T {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            return use(admin)
        } finally {
            admin.close()
        }
    }
}
