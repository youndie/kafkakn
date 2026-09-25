@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.set
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import platform.posix.size_tVar
import rdkafka.RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS
import rdkafka.RD_KAFKA_ADMIN_OP_CREATETOPICS
import rdkafka.RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS
import rdkafka.RD_KAFKA_ADMIN_OP_DELETEGROUPS
import rdkafka.RD_KAFKA_ADMIN_OP_DELETETOPICS
import rdkafka.RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER
import rdkafka.RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS
import rdkafka.RD_KAFKA_ADMIN_OP_DESCRIBETOPICS
import rdkafka.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS
import rdkafka.RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS
import rdkafka.RD_KAFKA_ADMIN_OP_LISTOFFSETS
import rdkafka.RD_KAFKA_CONF_OK
import rdkafka.RD_KAFKA_CONF_UNKNOWN
import rdkafka.RD_KAFKA_OFFSET_INVALID
import rdkafka.RD_KAFKA_OFFSET_SPEC_EARLIEST
import rdkafka.RD_KAFKA_OFFSET_SPEC_LATEST
import rdkafka.RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC
import rdkafka.RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP
import rdkafka.RD_KAFKA_RESP_ERR_NO_ERROR
import rdkafka.RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS
import rdkafka.RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID
import rdkafka.rd_kafka_AdminOptions_destroy
import rdkafka.rd_kafka_AdminOptions_new
import rdkafka.rd_kafka_AdminOptions_t
import rdkafka.rd_kafka_AlterConsumerGroupOffsets
import rdkafka.rd_kafka_AlterConsumerGroupOffsets_destroy
import rdkafka.rd_kafka_AlterConsumerGroupOffsets_new
import rdkafka.rd_kafka_AlterConsumerGroupOffsets_result_groups
import rdkafka.rd_kafka_AlterConsumerGroupOffsets_t
import rdkafka.rd_kafka_ConsumerGroupDescription_error
import rdkafka.rd_kafka_ConsumerGroupDescription_group_id
import rdkafka.rd_kafka_ConsumerGroupDescription_member
import rdkafka.rd_kafka_ConsumerGroupDescription_member_count
import rdkafka.rd_kafka_ConsumerGroupDescription_partition_assignor
import rdkafka.rd_kafka_ConsumerGroupDescription_state
import rdkafka.rd_kafka_ConsumerGroupListing_group_id
import rdkafka.rd_kafka_ConsumerGroupListing_state
import rdkafka.rd_kafka_CreateTopics
import rdkafka.rd_kafka_CreateTopics_result_topics
import rdkafka.rd_kafka_DeleteConsumerGroupOffsets
import rdkafka.rd_kafka_DeleteConsumerGroupOffsets_destroy
import rdkafka.rd_kafka_DeleteConsumerGroupOffsets_new
import rdkafka.rd_kafka_DeleteConsumerGroupOffsets_result_groups
import rdkafka.rd_kafka_DeleteConsumerGroupOffsets_t
import rdkafka.rd_kafka_DeleteGroup_destroy_array
import rdkafka.rd_kafka_DeleteGroup_new
import rdkafka.rd_kafka_DeleteGroup_t
import rdkafka.rd_kafka_DeleteGroups
import rdkafka.rd_kafka_DeleteGroups_result_groups
import rdkafka.rd_kafka_DeleteTopic_destroy_array
import rdkafka.rd_kafka_DeleteTopic_new
import rdkafka.rd_kafka_DeleteTopic_t
import rdkafka.rd_kafka_DeleteTopics
import rdkafka.rd_kafka_DeleteTopics_result_topics
import rdkafka.rd_kafka_DescribeCluster
import rdkafka.rd_kafka_DescribeCluster_result_cluster_id
import rdkafka.rd_kafka_DescribeCluster_result_controller
import rdkafka.rd_kafka_DescribeCluster_result_nodes
import rdkafka.rd_kafka_DescribeConsumerGroups
import rdkafka.rd_kafka_DescribeConsumerGroups_result_groups
import rdkafka.rd_kafka_DescribeTopics
import rdkafka.rd_kafka_DescribeTopics_result_topics
import rdkafka.rd_kafka_ListConsumerGroupOffsets
import rdkafka.rd_kafka_ListConsumerGroupOffsets_destroy
import rdkafka.rd_kafka_ListConsumerGroupOffsets_new
import rdkafka.rd_kafka_ListConsumerGroupOffsets_result_groups
import rdkafka.rd_kafka_ListConsumerGroupOffsets_t
import rdkafka.rd_kafka_ListConsumerGroups
import rdkafka.rd_kafka_ListConsumerGroups_result_valid
import rdkafka.rd_kafka_ListOffsets
import rdkafka.rd_kafka_ListOffsetsResultInfo_topic_partition
import rdkafka.rd_kafka_ListOffsets_result_infos
import rdkafka.rd_kafka_MemberAssignment_partitions
import rdkafka.rd_kafka_MemberDescription_assignment
import rdkafka.rd_kafka_MemberDescription_client_id
import rdkafka.rd_kafka_MemberDescription_consumer_id
import rdkafka.rd_kafka_MemberDescription_host
import rdkafka.rd_kafka_NewTopic_destroy_array
import rdkafka.rd_kafka_NewTopic_new
import rdkafka.rd_kafka_NewTopic_set_config
import rdkafka.rd_kafka_NewTopic_t
import rdkafka.rd_kafka_Node_host
import rdkafka.rd_kafka_Node_id
import rdkafka.rd_kafka_Node_port
import rdkafka.rd_kafka_Node_t
import rdkafka.rd_kafka_TopicCollection_destroy
import rdkafka.rd_kafka_TopicCollection_of_topic_names
import rdkafka.rd_kafka_TopicDescription_error
import rdkafka.rd_kafka_TopicDescription_name
import rdkafka.rd_kafka_TopicDescription_partitions
import rdkafka.rd_kafka_TopicPartitionInfo_isr
import rdkafka.rd_kafka_TopicPartitionInfo_leader
import rdkafka.rd_kafka_TopicPartitionInfo_partition
import rdkafka.rd_kafka_TopicPartitionInfo_replicas
import rdkafka.rd_kafka_admin_op_t
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
import rdkafka.rd_kafka_consumer_group_state_name
import rdkafka.rd_kafka_consumer_group_state_t
import rdkafka.rd_kafka_destroy
import rdkafka.rd_kafka_err2str
import rdkafka.rd_kafka_error_code
import rdkafka.rd_kafka_error_string
import rdkafka.rd_kafka_event_AlterConsumerGroupOffsets_result
import rdkafka.rd_kafka_event_CreateTopics_result
import rdkafka.rd_kafka_event_DeleteConsumerGroupOffsets_result
import rdkafka.rd_kafka_event_DeleteGroups_result
import rdkafka.rd_kafka_event_DeleteTopics_result
import rdkafka.rd_kafka_event_DescribeCluster_result
import rdkafka.rd_kafka_event_DescribeConsumerGroups_result
import rdkafka.rd_kafka_event_DescribeTopics_result
import rdkafka.rd_kafka_event_ListConsumerGroupOffsets_result
import rdkafka.rd_kafka_event_ListConsumerGroups_result
import rdkafka.rd_kafka_event_ListOffsets_result
import rdkafka.rd_kafka_event_destroy
import rdkafka.rd_kafka_event_error
import rdkafka.rd_kafka_event_error_string
import rdkafka.rd_kafka_event_t
import rdkafka.rd_kafka_group_result_error
import rdkafka.rd_kafka_group_result_name
import rdkafka.rd_kafka_group_result_partitions
import rdkafka.rd_kafka_group_result_t
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_queue_destroy
import rdkafka.rd_kafka_queue_new
import rdkafka.rd_kafka_queue_poll
import rdkafka.rd_kafka_queue_t
import rdkafka.rd_kafka_resp_err_t
import rdkafka.rd_kafka_t
import rdkafka.rd_kafka_topic_partition_list_add
import rdkafka.rd_kafka_topic_partition_list_destroy
import rdkafka.rd_kafka_topic_partition_list_new
import rdkafka.rd_kafka_topic_partition_list_t
import rdkafka.rd_kafka_topic_result_error
import rdkafka.rd_kafka_topic_result_error_string
import rdkafka.rd_kafka_topic_result_name
import rdkafka.rd_kafka_topic_result_t
import rdkafka.rd_kafka_type_t
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

public actual fun kafkaAdmin(config: AdminConfig): KafkaAdmin = NativeKafkaAdmin(config)

/** librdkafka refused an admin request, or its answer did not arrive. */
public class KafkaAdminException(
    message: String,
) : RuntimeException(message)

/**
 * The native admin client: a librdkafka handle used only for the `rd_kafka_*Topics` family.
 *
 * **Every answer arrives on an event queue**, the same seam as the producer's delivery reports, and
 * it is bridged the same way: polled with a zero timeout, `delay` between polls, so a caller waiting
 * for the broker holds no thread. Each request gets a queue of its own, which is what keeps two
 * concurrent calls from reading each other's answers.
 */
internal class NativeKafkaAdmin(
    config: AdminConfig,
) : KafkaAdmin {
    init {
        // The producer's rules, on the same keys: TLS and SASL are spelled and refused alike whichever
        // client carries them. ProducerConfig here is only the type the rules are written against.
        ProducerConfig(config.properties).run {
            checkTlsKeys()
            checkSaslKeys()
        }
    }

    private val handle: CPointer<rd_kafka_t> =
        memScoped {
            val conf = rd_kafka_conf_new() ?: error("rd_kafka_conf_new returned null")
            val errstr = allocArray<ByteVar>(ERRSTR)
            config.properties.forEach { (key, value) ->
                val result = rd_kafka_conf_set(conf, key, value, errstr, ERRSTR.convert())
                if (result == RD_KAFKA_CONF_UNKNOWN) {
                    throw IllegalArgumentException("unknown admin configuration: $key (${errstr.toKString()})")
                }
                if (result != RD_KAFKA_CONF_OK) {
                    throw IllegalArgumentException(
                        "admin configuration $key refuses the value '$value' (${errstr.toKString()})",
                    )
                }
            }
            // A PRODUCER handle, as librdkafka's own admin examples use: an admin client is not a type
            // of its own there, and a producer handle opens no group membership.
            rd_kafka_new(rd_kafka_type_t.RD_KAFKA_PRODUCER, conf, errstr, ERRSTR.convert())
                ?: error("rd_kafka_new failed: ${errstr.toKString()}")
        }

    /**
     * How long this side waits before calling an answer lost: the request timeout librdkafka applies
     * by default (`socket.timeout.ms`), plus the operation timeout a create or delete may spend on the
     * controller, plus a margin. librdkafka reports its own timeouts on the queue well before this; it
     * is a guard against a queue that never answers, not the timeout a caller meets.
     */
    private val giveUpAfter =
        (config.properties["socket.timeout.ms"]?.toLongOrNull() ?: DEFAULT_SOCKET_TIMEOUT_MS) +
            DEFAULT_OPERATION_TIMEOUT_MS + GUARD_MARGIN_MS

    override suspend fun createTopics(topics: List<NewTopic>) {
        request(
            RD_KAFKA_ADMIN_OP_CREATETOPICS,
            "createTopics",
            submit = { options, queue ->
                memScoped {
                    val errstr = allocArray<ByteVar>(ERRSTR)
                    val array = allocArray<CPointerVar<rd_kafka_NewTopic_t>>(topics.size)
                    var built = 0
                    try {
                        topics.forEachIndexed { index, topic ->
                            val created =
                                rd_kafka_NewTopic_new(
                                    topic.name,
                                    topic.partitions,
                                    topic.replicationFactor,
                                    errstr,
                                    ERRSTR.convert(),
                                )
                                    ?: throw IllegalArgumentException("${topic.name}: ${errstr.toKString()}")
                            array[index] = created
                            built++
                            topic.config.forEach { (key, value) -> rd_kafka_NewTopic_set_config(created, key, value) }
                        }
                        rd_kafka_CreateTopics(handle, array, topics.size.convert(), options, queue)
                    } finally {
                        // The request holds its own copy, as librdkafka's own tests rely on.
                        rd_kafka_NewTopic_destroy_array(array, built.convert())
                    }
                }
            },
            read = { event ->
                val result = rd_kafka_event_CreateTopics_result(event) ?: error("not a CreateTopics result")
                memScoped {
                    val count = alloc<size_tVar>()
                    checkTopicResults(
                        "createTopics",
                        rd_kafka_CreateTopics_result_topics(result, count.ptr),
                        count.value.toInt(),
                    )
                }
            },
        )
    }

    override suspend fun deleteTopics(names: List<String>) {
        request(
            RD_KAFKA_ADMIN_OP_DELETETOPICS,
            "deleteTopics",
            submit = { options, queue ->
                memScoped {
                    val array = allocArray<CPointerVar<rd_kafka_DeleteTopic_t>>(names.size)
                    names.forEachIndexed { index, name -> array[index] = rd_kafka_DeleteTopic_new(name) }
                    try {
                        rd_kafka_DeleteTopics(handle, array, names.size.convert(), options, queue)
                    } finally {
                        rd_kafka_DeleteTopic_destroy_array(array, names.size.convert())
                    }
                }
            },
            read = { event ->
                val result = rd_kafka_event_DeleteTopics_result(event) ?: error("not a DeleteTopics result")
                memScoped {
                    val count = alloc<size_tVar>()
                    checkTopicResults(
                        "deleteTopics",
                        rd_kafka_DeleteTopics_result_topics(result, count.ptr),
                        count.value.toInt(),
                    )
                }
            },
        )
    }

    override suspend fun describeTopics(names: List<String>): Map<String, List<PartitionInfo>> =
        request(
            RD_KAFKA_ADMIN_OP_DESCRIBETOPICS,
            "describeTopics",
            submit = { options, queue ->
                memScoped {
                    val array = allocArray<CPointerVar<ByteVar>>(names.size)
                    names.forEachIndexed { index, name -> array[index] = name.cstr.ptr }
                    val collection =
                        rd_kafka_TopicCollection_of_topic_names(array, names.size.convert())
                            ?: error("rd_kafka_TopicCollection_of_topic_names returned null")
                    try {
                        rd_kafka_DescribeTopics(handle, collection, options, queue)
                    } finally {
                        rd_kafka_TopicCollection_destroy(collection)
                    }
                }
            },
            read = { event ->
                val result = rd_kafka_event_DescribeTopics_result(event) ?: error("not a DescribeTopics result")
                memScoped {
                    val count = alloc<size_tVar>()
                    val described = rd_kafka_DescribeTopics_result_topics(result, count.ptr)
                    (0 until count.value.toInt()).associate { index ->
                        val topic = described!![index]!!
                        val name = rd_kafka_TopicDescription_name(topic)?.toKString() ?: "<unnamed>"
                        rd_kafka_TopicDescription_error(topic)?.let { error ->
                            throw KafkaAdminException(
                                "describeTopics: $name: ${rd_kafka_error_string(
                                    error,
                                )?.toKString()} (${rd_kafka_error_code(error)})",
                            )
                        }
                        val partitionCount = alloc<size_tVar>()
                        val partitions = rd_kafka_TopicDescription_partitions(topic, partitionCount.ptr)
                        name to
                            (0 until partitionCount.value.toInt())
                                .map { at ->
                                    val info = partitions!![at]!!
                                    val isrCount = alloc<size_tVar>()
                                    val isr = rd_kafka_TopicPartitionInfo_isr(info, isrCount.ptr)
                                    val replicaCount = alloc<size_tVar>()
                                    val replicas = rd_kafka_TopicPartitionInfo_replicas(info, replicaCount.ptr)
                                    PartitionInfo(
                                        topic = name,
                                        partition = rd_kafka_TopicPartitionInfo_partition(info),
                                        leader = rd_kafka_TopicPartitionInfo_leader(info)?.let { rd_kafka_Node_id(it) },
                                        replicas = nodeIds(replicas, replicaCount.value.toInt()),
                                        inSyncReplicas = nodeIds(isr, isrCount.value.toInt()),
                                    )
                                }.sortedBy { it.partition }
                    }
                }
            },
        )

    override suspend fun describeCluster(): ClusterDescription =
        request(
            RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER,
            "describeCluster",
            submit = { options, queue -> rd_kafka_DescribeCluster(handle, options, queue) },
            read = { event ->
                val result = rd_kafka_event_DescribeCluster_result(event) ?: error("not a DescribeCluster result")
                memScoped {
                    val count = alloc<size_tVar>()
                    val nodes = rd_kafka_DescribeCluster_result_nodes(result, count.ptr)
                    ClusterDescription(
                        clusterId = rd_kafka_DescribeCluster_result_cluster_id(result)?.toKString(),
                        controller = rd_kafka_DescribeCluster_result_controller(result)?.let { rd_kafka_Node_id(it) },
                        nodes =
                            (0 until count.value.toInt()).map { index ->
                                val node = nodes!![index]!!
                                BrokerNode(
                                    id = rd_kafka_Node_id(node),
                                    host = rd_kafka_Node_host(node)?.toKString() ?: "",
                                    port = rd_kafka_Node_port(node).toInt(),
                                )
                            },
                    )
                }
            },
        )

    override suspend fun listConsumerGroups(): List<ConsumerGroupListing> =
        request(
            RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
            "listConsumerGroups",
            submit = { options, queue -> rd_kafka_ListConsumerGroups(handle, options, queue) },
            read = { event ->
                val result = rd_kafka_event_ListConsumerGroups_result(event) ?: error("not a ListConsumerGroups result")
                memScoped {
                    val count = alloc<size_tVar>()
                    val valid = rd_kafka_ListConsumerGroups_result_valid(result, count.ptr)
                    (0 until count.value.toInt())
                        .map { index ->
                            val listing = valid!![index]!!
                            ConsumerGroupListing(
                                groupId = rd_kafka_ConsumerGroupListing_group_id(listing)?.toKString() ?: "<unnamed>",
                                state = stateOf(rd_kafka_ConsumerGroupListing_state(listing)),
                            )
                        }.sortedBy { it.groupId }
                }
            },
        )

    override suspend fun describeConsumerGroups(groupIds: List<String>): Map<String, ConsumerGroupDescription> =
        request(
            RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
            "describeConsumerGroups",
            submit = { options, queue ->
                memScoped {
                    val array = allocArray<CPointerVar<ByteVar>>(groupIds.size)
                    groupIds.forEachIndexed { index, id -> array[index] = id.cstr.ptr }
                    rd_kafka_DescribeConsumerGroups(handle, array, groupIds.size.convert(), options, queue)
                }
            },
            read = { event ->
                val result =
                    rd_kafka_event_DescribeConsumerGroups_result(event) ?: error("not a DescribeConsumerGroups result")
                memScoped {
                    val count = alloc<size_tVar>()
                    val groups = rd_kafka_DescribeConsumerGroups_result_groups(result, count.ptr)
                    (0 until count.value.toInt()).associate { index ->
                        val group = groups!![index]!!
                        val id = rd_kafka_ConsumerGroupDescription_group_id(group)?.toKString() ?: "<unnamed>"
                        rd_kafka_ConsumerGroupDescription_error(group)?.let { error ->
                            throw KafkaAdminException(
                                "describeConsumerGroups: $id: ${rd_kafka_error_string(
                                    error,
                                )?.toKString()} (${rd_kafka_error_code(error)})",
                            )
                        }
                        id to
                            ConsumerGroupDescription(
                                groupId = id,
                                state = stateOf(rd_kafka_ConsumerGroupDescription_state(group)),
                                partitionAssignor =
                                    rd_kafka_ConsumerGroupDescription_partition_assignor(
                                        group,
                                    )?.toKString().orEmpty(),
                                members =
                                    (0 until rd_kafka_ConsumerGroupDescription_member_count(group).toInt()).map { at ->
                                        val member = rd_kafka_ConsumerGroupDescription_member(group, at.convert())!!
                                        GroupMember(
                                            memberId =
                                                rd_kafka_MemberDescription_consumer_id(
                                                    member,
                                                )?.toKString().orEmpty(),
                                            clientId =
                                                rd_kafka_MemberDescription_client_id(
                                                    member,
                                                )?.toKString().orEmpty(),
                                            host =
                                                rd_kafka_MemberDescription_host(
                                                    member,
                                                )?.toKString().orEmpty(),
                                            assignment = assignmentOf(member),
                                        )
                                    },
                            )
                    }
                }
            },
        )

    override suspend fun alterConsumerGroupOffsets(
        groupId: String,
        offsets: Map<TopicPartition, Long>,
    ) {
        requireCommittable(offsets)
        request(
            RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
            "alterConsumerGroupOffsets",
            submit = { options, queue ->
                withPartitionList(offsets.keys.toList(), offsets::getValue) { list ->
                    memScoped {
                        val asked =
                            rd_kafka_AlterConsumerGroupOffsets_new(groupId, list)
                                ?: error("rd_kafka_AlterConsumerGroupOffsets_new returned null")
                        val array = allocArray<CPointerVar<rd_kafka_AlterConsumerGroupOffsets_t>>(1)
                        array[0] = asked
                        try {
                            rd_kafka_AlterConsumerGroupOffsets(handle, array, 1.convert(), options, queue)
                        } finally {
                            rd_kafka_AlterConsumerGroupOffsets_destroy(asked)
                        }
                    }
                }
            },
            read = { event ->
                val result =
                    rd_kafka_event_AlterConsumerGroupOffsets_result(event)
                        ?: error("not an AlterConsumerGroupOffsets result")
                memScoped {
                    val count = alloc<size_tVar>()
                    checkGroupResults(
                        "alterConsumerGroupOffsets",
                        rd_kafka_AlterConsumerGroupOffsets_result_groups(result, count.ptr),
                        count.value.toInt(),
                    )
                }
            },
        )
    }

    override suspend fun deleteConsumerGroupOffsets(
        groupId: String,
        partitions: List<TopicPartition>,
    ) {
        request(
            RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS,
            "deleteConsumerGroupOffsets",
            submit = { options, queue ->
                withPartitionList(partitions, { RD_KAFKA_OFFSET_INVALID.toLong() }) { list ->
                    memScoped {
                        val asked =
                            rd_kafka_DeleteConsumerGroupOffsets_new(groupId, list)
                                ?: error("rd_kafka_DeleteConsumerGroupOffsets_new returned null")
                        val array = allocArray<CPointerVar<rd_kafka_DeleteConsumerGroupOffsets_t>>(1)
                        array[0] = asked
                        try {
                            rd_kafka_DeleteConsumerGroupOffsets(handle, array, 1.convert(), options, queue)
                        } finally {
                            rd_kafka_DeleteConsumerGroupOffsets_destroy(asked)
                        }
                    }
                }
            },
            read = { event ->
                val result =
                    rd_kafka_event_DeleteConsumerGroupOffsets_result(event)
                        ?: error("not a DeleteConsumerGroupOffsets result")
                memScoped {
                    val count = alloc<size_tVar>()
                    checkGroupResults(
                        "deleteConsumerGroupOffsets",
                        rd_kafka_DeleteConsumerGroupOffsets_result_groups(result, count.ptr),
                        count.value.toInt(),
                    )
                }
            },
        )
    }

    override suspend fun deleteConsumerGroups(groupIds: List<String>) {
        request(
            RD_KAFKA_ADMIN_OP_DELETEGROUPS,
            "deleteConsumerGroups",
            submit = { options, queue ->
                memScoped {
                    val array = allocArray<CPointerVar<rd_kafka_DeleteGroup_t>>(groupIds.size)
                    groupIds.forEachIndexed { index, id -> array[index] = rd_kafka_DeleteGroup_new(id) }
                    try {
                        rd_kafka_DeleteGroups(handle, array, groupIds.size.convert(), options, queue)
                    } finally {
                        rd_kafka_DeleteGroup_destroy_array(array, groupIds.size.convert())
                    }
                }
            },
            read = { event ->
                val result = rd_kafka_event_DeleteGroups_result(event) ?: error("not a DeleteGroups result")
                memScoped {
                    val count = alloc<size_tVar>()
                    checkGroupResults(
                        "deleteConsumerGroups",
                        rd_kafka_DeleteGroups_result_groups(result, count.ptr),
                        count.value.toInt(),
                    )
                }
            },
        )
    }

    /** A partition list of [partitions], each carrying [offset] of itself, freed after [use]. */
    private fun <T> withPartitionList(
        partitions: List<TopicPartition>,
        offset: (TopicPartition) -> Long,
        use: (CPointer<rd_kafka_topic_partition_list_t>) -> T,
    ): T {
        val list =
            rd_kafka_topic_partition_list_new(partitions.size)
                ?: error("rd_kafka_topic_partition_list_new returned null")
        try {
            partitions.forEach { partition ->
                rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)!!.pointed.offset =
                    offset(partition)
            }
            return use(list)
        } finally {
            rd_kafka_topic_partition_list_destroy(list)
        }
    }

    /**
     * Per-group outcomes, and per-partition ones inside each: a request that succeeded as a whole can still
     * carry a refusal for one group, or for one partition of it.
     */
    private fun checkGroupResults(
        what: String,
        groups: CPointer<CPointerVar<rd_kafka_group_result_t>>?,
        count: Int,
    ) {
        for (index in 0 until count) {
            val group = groups!![index]!!
            val name = rd_kafka_group_result_name(group)?.toKString()
            rd_kafka_group_result_error(group)?.let { error ->
                throw refusal(
                    rd_kafka_error_code(error),
                    "$what: $name: ${rd_kafka_error_string(error)?.toKString()} (${rd_kafka_error_code(error)})",
                )
            }
            val list = rd_kafka_group_result_partitions(group) ?: continue
            for (at in 0 until list.pointed.cnt) {
                val entry = list.pointed.elems!![at]
                if (entry.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw refusal(
                        entry.err,
                        "$what: $name: ${entry.topic?.toKString()}-${entry.partition}: " +
                            "${rd_kafka_err2str(entry.err)?.toKString()} (${entry.err})",
                    )
                }
            }
        }
    }

    /**
     * The broker's refusal to touch a group with an active member, in the one type both arms throw (B-60):
     * `UNKNOWN_MEMBER_ID` to an altered offset (the admin commits as no member), `GROUP_SUBSCRIBED_TO_TOPIC`
     * to a deleted one, `NON_EMPTY_GROUP` to a deleted group. Anything else stays librdkafka's.
     */
    private fun refusal(
        code: rd_kafka_resp_err_t,
        message: String,
    ): Exception =
        when (code) {
            RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID,
            RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC,
            RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP,
            -> GroupNotEmptyException(message)

            else -> KafkaAdminException(message)
        }

    /** The member's assigned partitions, in topic-then-partition order. */
    private fun assignmentOf(member: CPointer<rdkafka.rd_kafka_MemberDescription_t>): List<TopicPartition> {
        val list =
            rd_kafka_MemberAssignment_partitions(rd_kafka_MemberDescription_assignment(member)) ?: return emptyList()
        return (0 until list.pointed.cnt)
            .map { index ->
                val entry = list.pointed.elems!![index]
                TopicPartition(entry.topic!!.toKString(), entry.partition)
            }.sortedWith(PARTITION_ORDER)
    }

    /** librdkafka's state, by its name (`Stable`, `PreparingRebalance`), into the one both arms report. */
    private fun stateOf(state: rd_kafka_consumer_group_state_t): GroupState =
        GroupState.named(rd_kafka_consumer_group_state_name(state)?.toKString())

    /**
     * `rd_kafka_ListConsumerGroupOffsets` with no partitions named: every partition the group committed. An
     * entry without a commit carries a negative (logical) offset, and is dropped as the JVM arm drops its null.
     */
    override suspend fun listConsumerGroupOffsets(groupId: String): Map<TopicPartition, Long> =
        request(
            RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
            "listConsumerGroupOffsets",
            submit = { options, queue ->
                memScoped {
                    val asked =
                        rd_kafka_ListConsumerGroupOffsets_new(groupId, null)
                            ?: error("rd_kafka_ListConsumerGroupOffsets_new returned null")
                    val array = allocArray<CPointerVar<rd_kafka_ListConsumerGroupOffsets_t>>(1)
                    array[0] = asked
                    try {
                        rd_kafka_ListConsumerGroupOffsets(handle, array, 1.convert(), options, queue)
                    } finally {
                        rd_kafka_ListConsumerGroupOffsets_destroy(asked)
                    }
                }
            },
            read = { event ->
                val result =
                    rd_kafka_event_ListConsumerGroupOffsets_result(event)
                        ?: error("not a ListConsumerGroupOffsets result")
                memScoped {
                    val count = alloc<size_tVar>()
                    val group = rd_kafka_ListConsumerGroupOffsets_result_groups(result, count.ptr)!![0]!!
                    rd_kafka_group_result_error(group)?.let { error ->
                        throw KafkaAdminException(
                            "listConsumerGroupOffsets: $groupId: ${rd_kafka_error_string(
                                error,
                            )?.toKString()} (${rd_kafka_error_code(error)})",
                        )
                    }
                    val list = rd_kafka_group_result_partitions(group)
                    (0 until (list?.pointed?.cnt ?: 0))
                        .mapNotNull { index ->
                            val entry = list!!.pointed.elems!![index]
                            val partition = TopicPartition(entry.topic!!.toKString(), entry.partition)
                            if (entry.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                                throw KafkaAdminException(
                                    "listConsumerGroupOffsets: $groupId: $partition: ${rd_kafka_err2str(
                                        entry.err,
                                    )?.toKString()}",
                                )
                            }
                            entry.offset.takeIf { it >= 0 }?.let { partition to it }
                        }.sortedWith(compareBy(PARTITION_ORDER) { it.first })
                        .toMap()
                }
            },
        )

    /**
     * `rd_kafka_ListOffsets`: each partition's offset field carries the question, a `rd_kafka_OffsetSpec_t` or
     * a timestamp. The answer is -1 when no record is as late as a timestamp: null here, as on the JVM.
     */
    override suspend fun listOffsets(
        partitions: List<TopicPartition>,
        spec: OffsetSpec,
    ): Map<TopicPartition, Long?> {
        val asked =
            when (spec) {
                OffsetSpec.Earliest -> RD_KAFKA_OFFSET_SPEC_EARLIEST.toLong()
                OffsetSpec.Latest -> RD_KAFKA_OFFSET_SPEC_LATEST.toLong()
                is OffsetSpec.Timestamp -> spec.timestamp
            }
        val answered =
            request(
                RD_KAFKA_ADMIN_OP_LISTOFFSETS,
                "listOffsets",
                submit = { options, queue ->
                    val list =
                        rd_kafka_topic_partition_list_new(partitions.size)
                            ?: error("rd_kafka_topic_partition_list_new returned null")
                    try {
                        partitions.forEach { partition ->
                            rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)!!
                                .pointed.offset = asked
                        }
                        rd_kafka_ListOffsets(handle, list, options, queue)
                    } finally {
                        rd_kafka_topic_partition_list_destroy(list)
                    }
                },
                read = { event ->
                    val result = rd_kafka_event_ListOffsets_result(event) ?: error("not a ListOffsets result")
                    memScoped {
                        val count = alloc<size_tVar>()
                        val infos = rd_kafka_ListOffsets_result_infos(result, count.ptr)
                        (0 until count.value.toInt()).associate { index ->
                            val entry = rd_kafka_ListOffsetsResultInfo_topic_partition(infos!![index]!!)!!.pointed
                            val partition = TopicPartition(entry.topic!!.toKString(), entry.partition)
                            if (entry.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                                throw KafkaAdminException(
                                    "listOffsets: $partition: ${rd_kafka_err2str(entry.err)?.toKString()}",
                                )
                            }
                            partition to entry.offset
                        }
                    }
                },
            )
        return partitions.sortedWith(PARTITION_ORDER).associateWith { partition ->
            val offset = answered[partition] ?: throw KafkaAdminException("listOffsets: no answer for $partition")
            offset.takeIf { it >= 0 }
        }
    }

    override suspend fun close() {
        // rd_kafka_destroy joins librdkafka's threads, briefly; on a thread that exists for waiting.
        withContext(Dispatchers.IO) { rd_kafka_destroy(handle) }
    }

    /**
     * One admin request: submit it to a queue of its own, poll that queue without holding a thread,
     * read the event, and free everything whatever happened.
     */
    private suspend fun <T> request(
        op: rd_kafka_admin_op_t,
        what: String,
        submit: (CPointer<rd_kafka_AdminOptions_t>, CPointer<rd_kafka_queue_t>) -> Unit,
        read: (CPointer<rd_kafka_event_t>) -> T,
    ): T {
        val queue = rd_kafka_queue_new(handle) ?: error("rd_kafka_queue_new returned null")
        try {
            val options = rd_kafka_AdminOptions_new(handle, op) ?: error("rd_kafka_AdminOptions_new returned null")
            try {
                submit(options, queue)
            } finally {
                rd_kafka_AdminOptions_destroy(options)
            }
            val started = TimeSource.Monotonic.markNow()
            var event = rd_kafka_queue_poll(queue, 0)
            while (event == null) {
                if (started.elapsedNow() > giveUpAfter.milliseconds) {
                    throw KafkaAdminException("$what: no answer on the result queue after ${started.elapsedNow()}")
                }
                delay(POLL_IDLE_MS)
                event = rd_kafka_queue_poll(queue, 0)
            }
            try {
                val err = rd_kafka_event_error(event)
                if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw KafkaAdminException("$what: ${rd_kafka_event_error_string(event)?.toKString()} ($err)")
                }
                return read(event)
            } finally {
                rd_kafka_event_destroy(event)
            }
        } finally {
            rd_kafka_queue_destroy(queue)
        }
    }

    /**
     * Per-topic outcomes of a create or delete: a request that succeeded as a whole can still carry a
     * refusal per topic, and an existing topic is the one this library names with a type of its own.
     */
    private fun checkTopicResults(
        what: String,
        results: CPointer<CPointerVar<rd_kafka_topic_result_t>>?,
        count: Int,
    ) {
        for (index in 0 until count) {
            val result = results!![index]!!
            val err = rd_kafka_topic_result_error(result)
            if (err == RD_KAFKA_RESP_ERR_NO_ERROR) continue
            val name = rd_kafka_topic_result_name(result)?.toKString()
            val said = rd_kafka_topic_result_error_string(result)?.toKString() ?: rd_kafka_err2str(err)?.toKString()
            if (err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
                throw TopicExistsException("$what: $name: $said")
            }
            throw KafkaAdminException("$what: $name: $said ($err)")
        }
    }

    private fun nodeIds(
        nodes: CPointer<CPointerVar<rd_kafka_Node_t>>?,
        count: Int,
    ): List<Int> = (0 until count).map { rd_kafka_Node_id(nodes!![it]!!) }

    private companion object {
        const val ERRSTR = 512
        const val POLL_IDLE_MS = 2L
        const val DEFAULT_SOCKET_TIMEOUT_MS = 60_000L

        /** librdkafka's default operation timeout for a create or delete, from its header. */
        const val DEFAULT_OPERATION_TIMEOUT_MS = 60_000L
        const val GUARD_MARGIN_MS = 10_000L
    }
}
