package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ListGroupsOptions
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.ApiException
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupSubscribedToTopicException
import org.apache.kafka.common.errors.UnknownMemberIdException
import java.util.Properties
import org.apache.kafka.clients.admin.NewTopic as ApacheNewTopic
import org.apache.kafka.clients.admin.OffsetSpec as ApacheOffsetSpec
import org.apache.kafka.common.TopicPartition as ApacheTopicPartition
import org.apache.kafka.common.errors.GroupNotEmptyException as ApacheGroupNotEmptyException
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

    override suspend fun listConsumerGroups(): List<ConsumerGroupListing> =
        // listGroups with consumer groups only: listConsumerGroups is deprecated in 4.3.1.
        answer("listConsumerGroups") { delegate.listGroups(ListGroupsOptions.forConsumerGroups()).valid() }
            .map {
                ConsumerGroupListing(
                    it.groupId(),
                    GroupState.named(
                        it
                            .groupState()
                            .map { state ->
                                state.name
                            }.orElse(null),
                    ),
                )
            }.sortedBy { it.groupId }

    /**
     * Each group's future on its own, so that one group that does not exist becomes its description rather
     * than the failure of the call. The Java client throws `GroupIdNotFoundException` for it, librdkafka
     * describes it as DEAD with no members and cannot tell a missing group from a dead one. So both arms
     * answer DEAD (B-58): a portable caller could not name the Java exception anyway.
     */
    override suspend fun describeConsumerGroups(groupIds: List<String>): Map<String, ConsumerGroupDescription> {
        val futures = delegate.describeConsumerGroups(groupIds).describedGroups()
        return groupIds.associateWith { id ->
            try {
                describedAs(id, futures.getValue(id).toCompletionStage().await())
            } catch (missing: GroupIdNotFoundException) {
                ConsumerGroupDescription(id, GroupState.DEAD, partitionAssignor = "", members = emptyList())
            }
        }
    }

    private fun describedAs(
        id: String,
        described: org.apache.kafka.clients.admin.ConsumerGroupDescription,
    ) = ConsumerGroupDescription(
        groupId = id,
        state = GroupState.named(described.groupState()?.name),
        partitionAssignor = described.partitionAssignor().orEmpty(),
        members =
            described.members().map { member ->
                GroupMember(
                    memberId = member.consumerId(),
                    clientId = member.clientId(),
                    host = member.host(),
                    assignment =
                        member
                            .assignment()
                            .topicPartitions()
                            .map { TopicPartition(it.topic(), it.partition()) }
                            .sortedWith(PARTITION_ORDER),
                )
            },
    )

    /** `partitionsToOffsetAndMetadata`: a partition asked for and never committed maps to null, and is dropped. */
    override suspend fun listConsumerGroupOffsets(groupId: String): Map<TopicPartition, Long> =
        answer(
            "listConsumerGroupOffsets",
        ) { delegate.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata() }
            .mapNotNull { (partition, committed) ->
                committed?.let { TopicPartition(partition.topic(), partition.partition()) to it.offset() }
            }.sortedWith(compareBy(PARTITION_ORDER) { it.first })
            .toMap()

    /** `ListOffsetsResultInfo.offset` is -1 when no record is as late as a timestamp asked for: null here. */
    override suspend fun listOffsets(
        partitions: List<TopicPartition>,
        spec: OffsetSpec,
    ): Map<TopicPartition, Long?> {
        val asked =
            when (spec) {
                OffsetSpec.Earliest -> ApacheOffsetSpec.earliest()
                OffsetSpec.Latest -> ApacheOffsetSpec.latest()
                is OffsetSpec.Timestamp -> ApacheOffsetSpec.forTimestamp(spec.timestamp)
            }
        val answered =
            answer("listOffsets") {
                delegate
                    .listOffsets(
                        partitions.associate { ApacheTopicPartition(it.topic, it.partition) to asked },
                    ).all()
            }
        return partitions.sortedWith(PARTITION_ORDER).associateWith { partition ->
            answered
                .getValue(ApacheTopicPartition(partition.topic, partition.partition))
                .offset()
                .takeIf { it >= 0 }
        }
    }

    override suspend fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, Long>,
    ) {
        requireCommittable(offsets)
        val moved = offsets.entries.associate { (partition, offset) -> partition.java() to OffsetAndMetadata(offset) }
        refusedWhileActive("alterConsumerGroupOffsets") {
            answer("alterConsumerGroupOffsets") { delegate.alterConsumerGroupOffsets(groupId, moved).all() }
        }
    }

    override suspend fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: List<TopicPartition>,
    ) {
        refusedWhileActive("deleteConsumerGroupOffsets") {
            answer("deleteConsumerGroupOffsets") {
                delegate.deleteConsumerGroupOffsets(groupId, partitions.map { it.java() }.toSet()).all()
            }
        }
    }

    override suspend fun deleteConsumerGroups(groupIds: List<String>) {
        refusedWhileActive("deleteConsumerGroups") {
            answer("deleteConsumerGroups") { delegate.deleteConsumerGroups(groupIds).all() }
        }
    }

    /**
     * The broker's refusal to touch a group that has an active member, in the one type both arms throw (B-60).
     * It says so three ways: `UNKNOWN_MEMBER_ID` to an altered offset (the admin commits as no member, and a
     * group with members accepts commits only from them), `GROUP_SUBSCRIBED_TO_TOPIC` to a deleted one, and
     * `NON_EMPTY_GROUP` to a deleted group.
     */
    private suspend fun <T> refusedWhileActive(
        what: String,
        call: suspend () -> T,
    ): T =
        try {
            call()
        } catch (refused: ApiException) {
            if (refused is UnknownMemberIdException ||
                refused is GroupSubscribedToTopicException ||
                refused is ApacheGroupNotEmptyException
            ) {
                throw GroupNotEmptyException("$what: ${refused.message}", refused)
            }
            throw refused
        }

    private fun TopicPartition.java() = ApacheTopicPartition(topic, partition)

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
