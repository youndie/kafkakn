package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeSource

/**
 * [B-34](../../../../../../../docs/backlog/B-34-a-minimal-admin.md): a minimal admin client.
 *
 * **Nothing here is checked by the library that did it.** A topic created through kafkakn is described
 * by `kafka-topics.sh` and `kafka-configs.sh` in `ci/b-34/run.sh`; a topic kafkakn describes is the
 * fixture's, created by `broker.sh`. The suite does not start building its fixtures with this client —
 * a library agreeing with itself is the shape CLAUDE.md forbids for produce and consume.
 */
class AdminTest {
    @Test
    fun a_created_topic_has_the_partitions_replication_and_configuration_asked_for() =
        runTest(timeout = TIMEOUT) {
            val name = "kafkakn-admin-created-$armName-${randomSuffix()}"
            withAdmin { it.createTopics(listOf(NewTopic(name, PARTITIONS, 1, TOPIC_CONFIG))) }
            // Described by the broker's own tools in the run script, against these values.
            recordArmFact("admin.created", name)
        }

    @Test
    fun a_deleted_topic_is_gone() =
        runTest(timeout = TIMEOUT) {
            val name = "kafkakn-admin-deleted-$armName-${randomSuffix()}"
            withAdmin { admin ->
                admin.createTopics(listOf(NewTopic(name, 1, 1)))
                admin.deleteTopics(listOf(name))
            }
            // Its absence is read by `kafka-topics.sh --list`; its creation, by the same run, is what
            // makes the absence mean deletion rather than a topic that never existed.
            recordArmFact("admin.deleted", name)
        }

    @Test
    fun describing_a_topic_agrees_with_the_broker() =
        runTest(timeout = TIMEOUT) {
            val described = withAdmin { it.describeTopics(listOf(metadataTopic)) }
            val partitions = described.getValue(metadataTopic)
            assertEquals((0 until metadataPartitions).toList(), partitions.map { it.partition })

            // The shape `TopicMetadataTest` records, so the run script compares both with one oracle.
            fun PartitionInfo.shape() =
                "$partition:$leader:${replicas.joinToString(",")}:${inSyncReplicas.joinToString(",")}"
            recordArmFact("admin.describe", partitions.joinToString(";") { it.shape() })
        }

    @Test
    fun creating_a_topic_that_exists_fails_with_one_kafkakn_exception_on_both_arms() =
        runTest(timeout = TIMEOUT) {
            val exists =
                withAdmin { admin ->
                    assertFailsWith<TopicExistsException> { admin.createTopics(listOf(NewTopic(testTopic, 1, 1))) }
                }
            recordArmFact("admin.exists.failure", exists.chainText().replace('\n', ' ').take(REASON))
        }

    @Test
    fun describing_the_cluster_names_its_id_and_its_broker() =
        runTest(timeout = TIMEOUT) {
            val cluster = withAdmin { it.describeCluster() }
            assertTrue(cluster.nodes.isNotEmpty(), "a cluster with no brokers: $cluster")
            assertTrue(cluster.nodes.any { it.port == BROKER_PORT }, "no broker on the port we connected to: $cluster")
            // The id is compared with `kafka-cluster.sh cluster-id`; the controller is recorded only,
            // because under KRaft what each client reports in that role is not promised to agree.
            recordArmFact("admin.cluster.id", cluster.clusterId.toString())
            recordArmFact("admin.cluster.nodes", cluster.nodes.joinToString(";") { "${it.id}@${it.host}:${it.port}" })
            recordArmFact("admin.cluster.controller", cluster.controller.toString())
        }

    /** The seam, held as `TopicMetadataTest` holds `partitionsFor` — mark before the call, read after. */
    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun no_call_holds_the_callers_dispatcher() =
        runTest(timeout = TIMEOUT) {
            val lane = Dispatchers.Default.limitedParallelism(1)
            var longestSilence = Duration.ZERO
            var outcome: String? = null
            val started = TimeSource.Monotonic.markNow()
            val admin = kafkaAdmin(AdminConfig(mapOf("bootstrap.servers" to "127.0.0.1:9099") + adminFailFastConfig()))
            try {
                withContext(lane) {
                    coroutineScope {
                        var lastTick = TimeSource.Monotonic.markNow()

                        fun tick() {
                            val gap = lastTick.elapsedNow()
                            if (gap > longestSilence) longestSilence = gap
                            lastTick = TimeSource.Monotonic.markNow()
                        }

                        val ticker =
                            launch {
                                while (isActive) {
                                    tick()
                                    delay(TICK)
                                }
                            }
                        outcome =
                            try {
                                admin.describeCluster()
                                "answered"
                            } catch (cancellation: CancellationException) {
                                throw cancellation
                            } catch (refused: Throwable) {
                                "${refused::class.simpleName}: ${refused.message}"
                            }
                        tick()
                        ticker.cancel()
                    }
                }
            } finally {
                withContext(Dispatchers.Default) { admin.close() }
            }
            val waited = started.elapsedNow()
            recordArmFact("admin.nowhere.ms", waited.inWholeMilliseconds.toString())
            recordArmFact("admin.nowhere.failure", outcome.toString().take(REASON))
            assertTrue(
                outcome != "answered" && waited > MEANINGFUL_WAIT,
                "the fixture stopped working: '$outcome' after $waited. Nothing below measures what it claims to.",
            )
            assertTrue(
                longestSilence < TOLERATED_SILENCE,
                "the caller's dispatcher was held for $longestSilence while describeCluster waited $waited",
            )
        }

    private suspend fun <T> withAdmin(use: suspend (KafkaAdmin) -> T): T {
        val admin = kafkaAdmin(AdminConfig("bootstrap.servers" to bootstrap))
        try {
            return use(admin)
        } finally {
            withContext(Dispatchers.Default) { admin.close() }
        }
    }

    private companion object {
        /** Five: a count neither the fixture's topics (3, 7) nor a default has. */
        const val PARTITIONS = 5
        val TOPIC_CONFIG = mapOf("retention.ms" to "123456789", "cleanup.policy" to "compact")
        const val BROKER_PORT = 9092
        const val REASON = 400
        val TIMEOUT = 2.minutes
        val TICK = 2.milliseconds
        val TOLERATED_SILENCE = 500.milliseconds
        val MEANINGFUL_WAIT = 2000.milliseconds
    }
}
