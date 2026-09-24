package io.github.youndie.kafkakn

/**
 * A minimal admin client ([B-34](../../../../../../../docs/backlog/B-34-a-minimal-admin.md)): create,
 * delete and describe topics, describe the cluster. `Admin` on the JVM, the `rd_kafka_CreateTopics`
 * family on native.
 *
 * **Separate from the producer, as both clients keep it.** Administration is a different lifecycle
 * and usually a different set of permissions; a producer that could delete topics would be carrying
 * a capability most of its callers must not have.
 *
 * Every call suspends and none holds the caller's dispatcher: the JVM arm awaits the client's futures,
 * the native arm polls its result queue the way the producer's pump polls delivery reports.
 */
public interface KafkaAdmin {
    /** Creates [topics]. Fails with [TopicExistsException] if one of them already exists. */
    public suspend fun createTopics(topics: List<NewTopic>)

    /** Deletes the topics named. */
    public suspend fun deleteTopics(names: List<String>)

    /** The partitions of each topic named, ordered by partition id — the shape `partitionsFor` returns. */
    public suspend fun describeTopics(names: List<String>): Map<String, List<PartitionInfo>>

    /** The cluster's id, its brokers and the one it reports as controller. */
    public suspend fun describeCluster(): ClusterDescription

    /** Releases the client. */
    public suspend fun close()
}

/** Creates an admin client. The implementation is the platform's, as for [kafkaProducer]. */
public expect fun kafkaAdmin(config: AdminConfig): KafkaAdmin

/**
 * The admin client's configuration, in Kafka's own keys, like [ProducerConfig] — and held to the same
 * rules: a key neither client honours fails at construction, and the TLS and SASL keys are spelled and
 * translated exactly as they are for a producer.
 */
public class AdminConfig(
    public val properties: Map<String, String>,
) {
    public constructor(vararg pairs: Pair<String, String>) : this(pairs.toMap())

    override fun toString(): String = "AdminConfig(${properties.keys.sorted()})"
}

/**
 * A topic to create. [config] is topic-level configuration in Kafka's own keys — `retention.ms`,
 * `cleanup.policy` — passed to the broker, which is the party that refuses a key it does not know.
 */
public data class NewTopic(
    public val name: String,
    public val partitions: Int,
    public val replicationFactor: Int,
    public val config: Map<String, String> = emptyMap(),
) {
    init {
        require(name.isNotEmpty()) { "a topic needs a name" }
        require(partitions > 0) { "partitions must be positive, not $partitions" }
        require(replicationFactor > 0) { "replicationFactor must be positive, not $replicationFactor" }
    }
}

/** A broker, as the cluster describes it. */
public data class BrokerNode(
    public val id: Int,
    public val host: String,
    public val port: Int,
)

/**
 * What the cluster says about itself. [controller] is whatever broker the cluster reports in that
 * role, or null; under KRaft that is not necessarily the node that runs the controller quorum, and
 * the two clients are not promised to agree on it.
 */
public data class ClusterDescription(
    public val clusterId: String?,
    public val controller: Int?,
    public val nodes: List<BrokerNode>,
)

/** A topic that was asked to be created already exists. One type on both arms; each client's own error is the [cause]. */
public class TopicExistsException(
    message: String,
    cause: Throwable? = null,
) : IllegalStateException(message, cause)
