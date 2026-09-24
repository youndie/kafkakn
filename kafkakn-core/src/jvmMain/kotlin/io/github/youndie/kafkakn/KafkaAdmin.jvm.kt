package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.KafkaFuture
import java.util.Properties
import org.apache.kafka.clients.admin.NewTopic as ApacheNewTopic
import org.apache.kafka.common.errors.TopicExistsException as ApacheTopicExistsException

/** The JVM arm of the admin client: `Admin`, delegated to, as the producer is. */
public actual fun kafkaAdmin(config: AdminConfig): KafkaAdmin = JvmKafkaAdmin(config)

internal class JvmKafkaAdmin(
    config: AdminConfig,
) : KafkaAdmin {
    init {
        // The producer's rules first, on the same keys, so that a key refused by decision says so
        // before it is reported as one the Java client does not know.
        ProducerConfig(config.properties).run {
            checkTlsKeys()
            checkSaslKeys()
        }
    }

    // The TLS and SASL translations are the producer's, unchanged: `ssl.ca.location` and the SASL
    // pair mean the same to an admin client, and a second spelling here would be the mistake the
    // contract exists to prevent.
    private val properties = translateForJava(config.properties)

    init {
        // As for the producer: a key `Admin` has never heard of fails here, instead of being logged at
        // WARN and ignored. The known set is the client's own.
        val known = AdminClientConfig.configNames()
        val unknown = properties.keys.filterNot { it in known }.sorted()
        require(unknown.isEmpty()) {
            "unknown admin configuration: ${unknown.joinToString()} " +
                "(kafka-clients knows ${known.size} admin keys; nothing here is silently ignored)"
        }
    }

    private val delegate =
        Admin.create(
            Properties().apply {
                properties.forEach { (key, value) -> setProperty(key, value) }
            },
        )

    override suspend fun createTopics(topics: List<NewTopic>) {
        val requested =
            topics.map { topic ->
                ApacheNewTopic(topic.name, topic.partitions, topic.replicationFactor.toShort()).configs(topic.config)
            }
        answer("createTopics") { delegate.createTopics(requested).all() }
    }

    override suspend fun deleteTopics(names: List<String>) {
        answer("deleteTopics") { delegate.deleteTopics(names).all() }
    }

    override suspend fun describeTopics(names: List<String>): Map<String, List<PartitionInfo>> =
        answer("describeTopics") { delegate.describeTopics(names).allTopicNames() }
            .mapValues { (name, description) ->
                description
                    .partitions()
                    .map { info ->
                        PartitionInfo(
                            topic = name,
                            partition = info.partition(),
                            leader = info.leader()?.takeUnless { it.isEmpty }?.id(),
                            replicas = info.replicas().map { it.id() },
                            inSyncReplicas = info.isr().map { it.id() },
                        )
                    }.sortedBy { it.partition }
            }

    override suspend fun describeCluster(): ClusterDescription {
        val described = delegate.describeCluster()
        return ClusterDescription(
            clusterId = answer("describeCluster") { described.clusterId() },
            controller = answer("describeCluster") { described.controller() }?.takeUnless { it.isEmpty }?.id(),
            nodes = answer("describeCluster") { described.nodes() }.map { BrokerNode(it.id(), it.host(), it.port()) },
        )
    }

    override suspend fun close() {
        // `close` waits for pending requests; on a thread that exists for waiting.
        withContext(Dispatchers.IO) { delegate.close() }
    }

    /**
     * Awaits one of the client's futures without holding a thread — `Admin`'s calls return at once and
     * complete the future on the client's own network thread — and turns an existing topic into the
     * one type both arms throw for it.
     */
    private suspend fun <T> answer(
        what: String,
        call: () -> KafkaFuture<T>,
    ): T =
        try {
            call().toCompletionStage().await()
        } catch (failure: ApacheTopicExistsException) {
            throw TopicExistsException("$what: ${failure.message}", failure)
        }
}
