package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.minutes

/**
 * [B-104](../../../../../../../docs/backlog/B-104-every-admin-failure-is-a-common-type.md): an admin failure kafkakn
 * does not name is [KafkaAdminException] on both arms, not a platform type. The one this suite can cause on demand is
 * a timeout: a broker that is not there, with each arm's own admin timeout short (`adminFailFastConfig`). The JVM times
 * out in the Java client's `TimeoutException`; native in librdkafka's own words.
 */
class AdminUnnamedFailureTest {
    @Test
    fun an_admin_request_to_a_broker_that_is_not_there_is_a_kafka_admin_exception_on_both_arms() =
        runTest(timeout = 2.minutes) {
            withContext(Dispatchers.Default) {
                val admin = kafkaAdmin(AdminConfig(mapOf("bootstrap.servers" to NOWHERE) + adminFailFastConfig()))
                try {
                    val cluster = assertFailsWith<KafkaAdminException> { admin.describeCluster() }
                    val topics = assertFailsWith<KafkaAdminException> { admin.describeTopics(listOf("kafkakn")) }
                    recordArmFact(
                        "admin.unnamed.cluster",
                        "${cluster.message} <- ${cluster.cause?.let { it::class.simpleName }}",
                    )
                    recordArmFact(
                        "admin.unnamed.topics",
                        "${topics.message} <- ${topics.cause?.let { it::class.simpleName }}",
                    )
                    recordObservation("admin.unnamed.type", "${cluster::class.simpleName} ${topics::class.simpleName}")
                } finally {
                    admin.close()
                }
            }
        }

    private companion object {
        const val NOWHERE = "127.0.0.1:9099"
    }
}
