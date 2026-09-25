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

    /**
     * Every consumer group the cluster has, with its state
     * ([B-58](../../../../../../../docs/backlog/B-58-list-and-describe-consumer-groups.md)). Consumer groups
     * only: the Java client's `listGroups` also lists share and streams groups, and both arms are asked
     * the same question.
     */
    public suspend fun listConsumerGroups(): List<ConsumerGroupListing>

    /** Each group named: its state, its assignor, and its members with what each is assigned. */
    public suspend fun describeConsumerGroups(groupIds: List<String>): Map<String, ConsumerGroupDescription>

    /**
     * What group [groupId] has committed, per partition: the next offset it will read
     * ([B-59](../../../../../../../docs/backlog/B-59-consumer-group-offsets-and-lag.md)), in topic-then-partition
     * order. Asked from outside the group, so no member is needed and none is disturbed. A partition the group
     * never committed is absent, and a group that does not exist answers an empty map.
     *
     * A group's lag is the caller's subtraction: [listOffsets] with [OffsetSpec.Latest], minus this.
     */
    public suspend fun listConsumerGroupOffsets(groupId: String): Map<TopicPartition, Long>

    /**
     * The broker's offset for each of [partitions] under [spec], the answer `kafka-get-offsets.sh` prints:
     * where the partition starts, the offset after its last record, or the first offset at or after a
     * timestamp. Null only for [OffsetSpec.Timestamp] when no record is that late. Read uncommitted, as both
     * clients default to: the end counts records of a transaction still open.
     */
    public suspend fun listOffsets(
        partitions: List<TopicPartition>,
        spec: OffsetSpec,
    ): Map<TopicPartition, Long?>

    /** Releases the client. */
    public suspend fun close()
}

/** A consumer group as a listing names it. */
public data class ConsumerGroupListing(
    public val groupId: String,
    public val state: GroupState,
)

/**
 * A consumer group as the cluster describes it. [partitionAssignor] is the protocol name the group settled
 * on (`range`, `cooperative-sticky`), empty while the group has no members.
 */
public data class ConsumerGroupDescription(
    public val groupId: String,
    public val state: GroupState,
    public val partitionAssignor: String,
    public val members: List<GroupMember>,
)

/** One member of a group, and the partitions it holds, in topic-then-partition order. */
public data class GroupMember(
    public val memberId: String,
    public val clientId: String,
    public val host: String,
    public val assignment: List<TopicPartition>,
)

/**
 * A group's state, in one set of names for both arms. The Java client's `GroupState` and librdkafka's
 * `rd_kafka_consumer_group_state_t` share these; a state only one of them knows reads [UNKNOWN].
 */
public enum class GroupState {
    UNKNOWN,
    PREPARING_REBALANCE,
    COMPLETING_REBALANCE,
    STABLE,
    DEAD,
    EMPTY,
    ;

    internal companion object {
        /** `STABLE`, `Stable` or `PreparingRebalance`: each client's spelling, into one. */
        fun named(name: String?): GroupState {
            val normalised = name.orEmpty().replace(Regex("([a-z])([A-Z])"), "$1_$2").uppercase()
            return entries.firstOrNull { it.name == normalised } ?: UNKNOWN
        }
    }
}

/**
 * Which offset [KafkaAdmin.listOffsets] asks for: both clients' `OffsetSpec` and `rd_kafka_OffsetSpec_t`,
 * without the max-timestamp variant, which answers a different question.
 */
public sealed interface OffsetSpec {
    /** The earliest offset the broker still has. */
    public data object Earliest : OffsetSpec

    /** The offset after the last record: the partition's end. */
    public data object Latest : OffsetSpec

    /** The first offset whose record's timestamp is at or after [timestamp], in milliseconds since the epoch. */
    public data class Timestamp(
        public val timestamp: Long,
    ) : OffsetSpec {
        init {
            // librdkafka reads a negative value as one of its specs (-1 latest, -2 earliest): a mistaken
            // timestamp would silently become a different question.
            require(timestamp >= 0) { "timestamp must not be negative, was $timestamp" }
        }
    }
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
