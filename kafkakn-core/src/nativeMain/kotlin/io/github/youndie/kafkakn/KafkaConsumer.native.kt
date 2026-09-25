@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.COpaquePointerVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.LongVar
import kotlinx.cinterop.StableRef
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.asStableRef
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import platform.posix.size_tVar
import rdkafka.RD_KAFKA_CONF_OK
import rdkafka.RD_KAFKA_CONF_UNKNOWN
import rdkafka.RD_KAFKA_RESP_ERR_NO_ERROR
import rdkafka.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
import rdkafka.RD_KAFKA_RESP_ERR__NO_OFFSET
import rdkafka.RD_KAFKA_RESP_ERR__PARTITION_EOF
import rdkafka.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
import rdkafka.rd_kafka_assign
import rdkafka.rd_kafka_assignment
import rdkafka.rd_kafka_assignment_lost
import rdkafka.rd_kafka_commit
import rdkafka.rd_kafka_committed
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
import rdkafka.rd_kafka_conf_set_opaque
import rdkafka.rd_kafka_conf_set_rebalance_cb
import rdkafka.rd_kafka_consumer_close
import rdkafka.rd_kafka_consumer_group_metadata
import rdkafka.rd_kafka_consumer_group_metadata_destroy
import rdkafka.rd_kafka_consumer_group_metadata_write
import rdkafka.rd_kafka_consumer_poll
import rdkafka.rd_kafka_destroy
import rdkafka.rd_kafka_err2str
import rdkafka.rd_kafka_error_destroy
import rdkafka.rd_kafka_error_string
import rdkafka.rd_kafka_header_cnt
import rdkafka.rd_kafka_header_get_all
import rdkafka.rd_kafka_headers_t
import rdkafka.rd_kafka_incremental_assign
import rdkafka.rd_kafka_incremental_unassign
import rdkafka.rd_kafka_mem_free
import rdkafka.rd_kafka_message_destroy
import rdkafka.rd_kafka_message_headers
import rdkafka.rd_kafka_message_t
import rdkafka.rd_kafka_message_timestamp
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_offsets_for_times
import rdkafka.rd_kafka_poll_set_consumer
import rdkafka.rd_kafka_position
import rdkafka.rd_kafka_query_watermark_offsets
import rdkafka.rd_kafka_rebalance_protocol
import rdkafka.rd_kafka_resp_err_t
import rdkafka.rd_kafka_seek_partitions
import rdkafka.rd_kafka_subscribe
import rdkafka.rd_kafka_t
import rdkafka.rd_kafka_topic_name
import rdkafka.rd_kafka_topic_partition_list_add
import rdkafka.rd_kafka_topic_partition_list_destroy
import rdkafka.rd_kafka_topic_partition_list_new
import rdkafka.rd_kafka_topic_partition_list_t
import rdkafka.rd_kafka_type_t
import kotlin.time.Duration
import kotlin.time.TimeSource

public actual fun kafkaConsumer(config: ConsumerConfig): KafkaConsumer = NativeKafkaConsumer(config)

private fun randomToken(): String =
    kotlin.random.Random
        .nextLong()
        .toULong()
        .toString(RANDOM_RADIX)

private const val RANDOM_RADIX = 36

/** librdkafka refused a consumer operation, or handed back an error instead of a record. */
public class KafkaConsumeException(
    message: String,
) : RuntimeException(message)

/**
 * The native arm of the consumer (consumer-contract §1).
 *
 * librdkafka is thread-safe, so nothing here confines calls to a thread. What it does do is **never
 * wait inside C** where it can help it: `poll` asks `rd_kafka_consumer_poll` with a zero timeout and
 * `delay`s between empty answers — the producer's delivery-report pump, again — so a waiting `poll`
 * holds no thread and cancellation reaches it at the next `delay`.
 *
 * Calls are still serialised, by a [Mutex] rather than a lane: not because librdkafka needs it, but
 * because the contract promises both arms queue overlapping calls rather than interleave them, and a
 * `seek` landing in the middle of another caller's drain would hand that caller records from two
 * positions in one list.
 */
internal class NativeKafkaConsumer(
    config: ConsumerConfig,
) : KafkaConsumer {
    init {
        ProducerConfig(config.properties).run {
            checkTlsKeys()
            checkSaslKeys()
        }
    }

    /**
     * The contract's defaults, and one more that only this arm needs.
     *
     * **librdkafka will not `assign` without a `group.id`** — measured 2026-09-24: `rd_kafka_assign`
     * answers `Local: Unknown group`, because assignment goes through its group machinery even when no
     * group is joined. The Java client assigns without one. So a caller who did not name a group gets
     * a private one here, `kafkakn-assign-<random>`: with auto-commit off and no commit made, it joins
     * nothing and commits nothing, and `ci/b-36/run.sh` asks `kafka-consumer-groups.sh --list` to confirm
     * the cluster never saw it. A caller who names a group keeps theirs.
     */
    private val namesAGroup = config.namesAGroup()

    private val properties =
        config.withContractDefaults().let { settled ->
            if ("group.id" in settled) settled else settled + ("group.id" to "kafkakn-assign-${randomToken()}")
        }

    /** What the rebalance callback needs, reached through librdkafka's opaque. Disposed after destroy. */
    private val bridge = RebalanceBridge(requestTimeoutMsFor(properties))
    private val bridgeRef = StableRef.create(bridge)

    private val handle: CPointer<rd_kafka_t> =
        memScoped {
            val conf = rd_kafka_conf_new() ?: error("rd_kafka_conf_new returned null")
            val errstr = allocArray<ByteVar>(ERRSTR)
            properties.forEach { (key, value) ->
                val result = rd_kafka_conf_set(conf, key, value, errstr, ERRSTR.convert())
                if (result == RD_KAFKA_CONF_UNKNOWN) {
                    throw IllegalArgumentException("unknown consumer configuration: $key (${errstr.toKString()})")
                }
                if (result != RD_KAFKA_CONF_OK) {
                    throw IllegalArgumentException(
                        "consumer configuration $key refuses the value '$value' (${errstr.toKString()})",
                    )
                }
            }
            // Always a rebalance callback, listener or not: it has to be on the configuration before
            // rd_kafka_new. Without a listener it applies each change exactly as librdkafka would by
            // itself (consumer-contract §2a).
            rd_kafka_conf_set_opaque(conf, bridgeRef.asCPointer())
            rd_kafka_conf_set_rebalance_cb(conf, staticCFunction(::onRebalance))
            val created =
                rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER, conf, errstr, ERRSTR.convert())
                    ?: error("rd_kafka_new failed: ${errstr.toKString()}")
            // Everything the consumer is told arrives on its one queue, which `rd_kafka_consumer_poll`
            // serves; without this the main queue would need a poll of its own.
            rd_kafka_poll_set_consumer(created)
            created
        }

    private val serial = Mutex()

    /** librdkafka's own bound for the one blocking call here, offsets for a time. */
    private val requestTimeoutMs = properties["socket.timeout.ms"]?.toIntOrNull() ?: DEFAULT_SOCKET_TIMEOUT_MS

    /** What is assigned, and where each partition is read from next, as far as this side knows. */
    private var assigned: List<TopicPartition> = emptyList()
    private val positions = mutableMapOf<TopicPartition, Long>()

    private fun enter(call: String) {
        if (Reentry.inside) refuseReentry(call)
    }

    /** A listener's exception, kept by the C callback that could not throw it, thrown by the call it ran in. */
    private fun rethrowFromCallback() {
        bridge.failure?.let {
            bridge.failure = null
            throw it
        }
    }

    override suspend fun assign(partitions: List<TopicPartition>) {
        enter("assign")
        serial.withLock {
            assigned = partitions
            positions.clear()
            reassign()
        }
    }

    /** Whether [subscribe] was the last of the two ways to get partitions. */
    private var subscribed = false

    override suspend fun subscribe(topics: List<String>) {
        enter("subscribe")
        bridge.listener = null
        subscribeTo(topics)
    }

    override suspend fun subscribe(
        topics: List<String>,
        listener: RebalanceListener,
    ) {
        enter("subscribe")
        bridge.listener = listener
        subscribeTo(topics)
    }

    private suspend fun subscribeTo(topics: List<String>) {
        requireGroup(namesAGroup, "subscribe")
        serial.withLock {
            // The rebalance callback (onRebalance, below) assigns and revokes, eager or incremental as the
            // group's protocol requires. With auto-commit off, a revoked partition resumes elsewhere from
            // its last commit: the at-least-once the contract promises, whether or not a listener commits.
            withPartitionList(topics.map { TopicPartition(it, 0) to OFFSET_INVALID }, anyPartition = true) { list ->
                val err = rd_kafka_subscribe(handle, list)
                if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw KafkaConsumeException("subscribe: ${rd_kafka_err2str(err)?.toKString()}")
                }
            }
            subscribed = true
        }
    }

    override suspend fun commit() {
        enter("commit")
        requireGroup(namesAGroup, "commit")
        serial.withLock {
            // NULL offsets: the stored ones, which `enable.auto.offset.store` fills as each record is
            // handed out - the position after everything `poll` returned, as the Java client's
            // commitSync(). Synchronous, so it waits for the coordinator, on a thread that exists for it.
            val err = withContext(Dispatchers.IO) { rd_kafka_commit(handle, null, 0) }
            // Nothing handed out since the last commit is not a failure; the Java client says nothing.
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR && err != RD_KAFKA_RESP_ERR__NO_OFFSET) {
                throw KafkaConsumeException("commit: ${rd_kafka_err2str(err)?.toKString()}")
            }
        }
    }

    override suspend fun commit(offsets: Map<TopicPartition, Long>) {
        enter("commit")
        requireGroup(namesAGroup, "commit")
        requireCommittable(offsets)
        if (offsets.isEmpty()) return
        serial.withLock {
            // Synchronous, as commit() is, on a thread that exists for waiting.
            withContext(Dispatchers.IO) { commitNow(handle, offsets) }
        }
    }

    /**
     * librdkafka's group metadata, serialised: `rd_kafka_consumer_group_metadata_write` exists "for
     * client binding use", and bytes need no lifetime managed across the consumer and the producer. The
     * producer reads them back for the one call that needs the object (B-38).
     */
    override suspend fun groupMetadata(): ConsumerGroupMetadata {
        enter("groupMetadata")
        requireGroup(namesAGroup, "groupMetadata")
        return serial.withLock {
            val metadata =
                rd_kafka_consumer_group_metadata(handle) ?: throw KafkaConsumeException("groupMetadata: none")
            try {
                memScoped {
                    val buffer = alloc<COpaquePointerVar>()
                    val size = alloc<size_tVar>()
                    rd_kafka_consumer_group_metadata_write(metadata, buffer.ptr, size.ptr)?.let { error ->
                        val said = rd_kafka_error_string(error)?.toKString()
                        rd_kafka_error_destroy(error)
                        throw KafkaConsumeException("groupMetadata: $said")
                    }
                    val bytes = buffer.value!!.readBytes(size.value.toInt())
                    rd_kafka_mem_free(null, buffer.value)
                    NativeGroupMetadata(bytes, properties.getValue("group.id"))
                }
            } finally {
                rd_kafka_consumer_group_metadata_destroy(metadata)
            }
        }
    }

    override suspend fun assignment(): List<TopicPartition> {
        enter("assignment")
        return assignmentNow()
    }

    private suspend fun assignmentNow(): List<TopicPartition> = serial.withLock { heldNow() }

    /** What librdkafka says this consumer holds, for a caller that already holds [serial]. */
    private fun heldNow(): List<TopicPartition> =
        memScoped {
            val held = alloc<CPointerVar<rd_kafka_topic_partition_list_t>>()
            val err = rd_kafka_assignment(handle, held.ptr)
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw KafkaConsumeException("assignment: ${rd_kafka_err2str(err)?.toKString()}")
            }
            val list = held.value ?: return@memScoped emptyList()
            try {
                (0 until list.pointed.cnt)
                    .map { index ->
                        val entry = list.pointed.elems!![index]
                        TopicPartition(entry.topic!!.toKString(), entry.partition)
                    }.sortedWith(PARTITION_ORDER)
            } finally {
                rd_kafka_topic_partition_list_destroy(list)
            }
        }

    /**
     * A seek is an ASSIGNMENT here, not `rd_kafka_seek_partitions`, and that was measured rather than
     * chosen. librdkafka's header says a seek "must only be performed for already assigned/consumed
     * partitions, use rd_kafka_assign() to set the initial starting offset" — and a seek right after
     * `assign`, before its fetcher had started, answered `Local: Erroneous state` (2026-09-24). The
     * Java client seeks at any moment after `assign`.
     *
     * So this arm keeps each assigned partition's next offset (updated as records are handed out) and
     * re-assigns the whole set with the one partition moved. A partition it knows nothing about yet
     * goes back as "invalid", which is where it started: the committed offset or `auto.offset.reset`.
     */
    override suspend fun seek(
        partition: TopicPartition,
        to: SeekTo,
    ) {
        enter("seek")
        serial.withLock {
            val held = if (subscribed) heldNow() else assigned
            check(partition in held) { "seek: $partition is not held by this consumer" }
            // A timestamp is a blocking call to the broker, on a thread that exists for it.
            val offset = withContext(Dispatchers.IO) { seekTarget(handle, partition, to, requestTimeoutMs) }
            if (subscribed) {
                // In a group the assignment is the group's, so this seeks rather than re-assigns (B-51), and
                // remembers where, so that position() answers it until a record is read.
                seekPartitionsNow(handle, partition, offset, requestTimeoutMs)
                bridge.groupSeeks[partition] = offset
            } else {
                positions[partition] = offset
                reassign()
            }
        }
    }

    private fun reassign() {
        withPartitionList(assigned.map { it to (positions[it] ?: OFFSET_INVALID) }) { list ->
            val err = rd_kafka_assign(handle, list)
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw KafkaConsumeException("assign: ${rd_kafka_err2str(err)?.toKString()}")
            }
        }
    }

    override suspend fun position(partition: TopicPartition): Long {
        enter("position")
        return serial.withLock {
            check(partition in heldNow()) { "position: $partition is not assigned to this consumer" }
            // Every source below may ask the broker, so on a thread that exists for waiting.
            withContext(Dispatchers.IO) { resolvePosition(partition) }
        }
    }

    /**
     * The offset the next poll returns, answered the way the Java client answers it, because
     * `rd_kafka_position` alone has none before the first record (`rdkafka.h`: "RD_KAFKA_OFFSET_INVALID in
     * case there was no previous message"). In order:
     * - with `assign`: [positions], which consumption and seeks both write, with a seek to the beginning
     *   or the end resolved through the watermarks;
     * - in a group: librdkafka's own position once a record was consumed. [positions] would be stale
     *   across a rebalance, so it is not used;
     * - what the group committed;
     * - where `auto.offset.reset` says.
     */
    private fun resolvePosition(partition: TopicPartition): Long {
        if (!subscribed) {
            positions[partition]?.let { known ->
                return when (known) {
                    OFFSET_BEGINNING -> watermarks(partition).first
                    OFFSET_END -> watermarks(partition).second
                    else -> known
                }
            }
        } else {
            // A seek in the group, not yet overtaken by a record: where it put the partition.
            bridge.groupSeeks[partition]?.let { sought ->
                return when (sought) {
                    OFFSET_BEGINNING -> watermarks(partition).first
                    OFFSET_END -> watermarks(partition).second
                    else -> sought
                }
            }
            consumedPosition(partition)?.let { return it }
        }
        committedNow(listOf(partition))[partition]?.let { return it }
        val (low, high) = watermarks(partition)
        return when (val reset = properties["auto.offset.reset"] ?: "latest") {
            "earliest", "smallest", "beginning" -> low

            "latest", "largest", "end" -> high

            else -> throw IllegalStateException(
                "position: nothing committed for $partition, and auto.offset.reset=$reset",
            )
        }
    }

    private fun consumedPosition(partition: TopicPartition): Long? =
        withPartitionList(listOf(partition to OFFSET_INVALID)) { list ->
            val err = rd_kafka_position(handle, list)
            if (err !=
                RD_KAFKA_RESP_ERR_NO_ERROR
            ) {
                throw KafkaConsumeException("position: ${rd_kafka_err2str(err)?.toKString()}")
            }
            list.pointed.elems!![0]
                .offset
                .takeIf { it >= 0 }
        }

    /** `rd_kafka_query_watermark_offsets`: the earliest offset the broker still has, and the end. */
    private fun watermarks(partition: TopicPartition): Pair<Long, Long> =
        memScoped {
            val low = alloc<LongVar>()
            val high = alloc<LongVar>()
            val err =
                rd_kafka_query_watermark_offsets(
                    handle,
                    partition.topic,
                    partition.partition,
                    low.ptr,
                    high.ptr,
                    requestTimeoutMs,
                )
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw KafkaConsumeException("watermarks of $partition: ${rd_kafka_err2str(err)?.toKString()}")
            }
            low.value to high.value
        }

    override suspend fun committed(partitions: List<TopicPartition>): Map<TopicPartition, Long?> {
        enter("committed")
        requireGroup(namesAGroup, "committed")
        if (partitions.isEmpty()) return emptyMap()
        return serial.withLock { withContext(Dispatchers.IO) { committedNow(partitions) } }
    }

    /** `rd_kafka_committed`: the group's committed offset per partition, or null where there is none. */
    private fun committedNow(partitions: List<TopicPartition>): Map<TopicPartition, Long?> =
        withPartitionList(partitions.map { it to OFFSET_INVALID }) { list ->
            val err = rd_kafka_committed(handle, list, requestTimeoutMs)
            if (err !=
                RD_KAFKA_RESP_ERR_NO_ERROR
            ) {
                throw KafkaConsumeException("committed: ${rd_kafka_err2str(err)?.toKString()}")
            }
            (0 until list.pointed.cnt).associate { index ->
                val entry = list.pointed.elems!![index]
                TopicPartition(entry.topic!!.toKString(), entry.partition) to entry.offset.takeIf { it >= 0 }
            }
        }

    override suspend fun poll(timeout: Duration): List<ConsumerRecord> {
        enter("poll")
        return serial.withLock { drain(timeout) }
    }

    /** Drains what is here, up to the contract's bound; if nothing is, waits by `delay`, never in C. */
    private suspend fun drain(timeout: Duration): List<ConsumerRecord> {
        val started = TimeSource.Monotonic.markNow()
        val records = mutableListOf<ConsumerRecord>()
        while (true) {
            while (records.size < MAX_RECORDS) {
                val message = rd_kafka_consumer_poll(handle, 0)
                // A rebalance callback runs inside that call; what its listener threw surfaces here.
                rethrowFromCallback()
                if (message == null) break
                try {
                    read(message)?.let { record ->
                        records += record
                        val from = TopicPartition(record.topic, record.partition)
                        positions[from] = record.offset + 1
                        bridge.groupSeeks.remove(from)
                    }
                } finally {
                    rd_kafka_message_destroy(message)
                }
            }
            if (records.isNotEmpty() || started.elapsedNow() >= timeout) return records
            delay(POLL_IDLE_MS)
        }
    }

    /** One message: a record, nothing (a partition end), or an exception carrying librdkafka's words. */
    private fun read(message: CPointer<rd_kafka_message_t>): ConsumerRecord? {
        val m = message.pointed
        if (m.err == RD_KAFKA_RESP_ERR__PARTITION_EOF) return null
        if (m.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw KafkaConsumeException("poll: ${rd_kafka_err2str(m.err)?.toKString()}")
        }
        return ConsumerRecord(
            topic = rd_kafka_topic_name(m.rkt)?.toKString() ?: "<unknown topic>",
            partition = m.partition,
            offset = m.offset,
            timestamp = rd_kafka_message_timestamp(message, null),
            // A NULL pointer is no key; a pointer with length 0 is an empty one. The two are different
            // records, and the fixture carries both.
            key = m.key?.readBytes(m.key_len.toInt()),
            value = m.payload?.readBytes(m.len.toInt()),
            headers = headersOf(message),
        )
    }

    /** The headers in order, duplicates kept, a null value kept as null. */
    private fun headersOf(message: CPointer<rd_kafka_message_t>): List<RecordHeader> =
        memScoped {
            val headers = alloc<CPointerVar<rd_kafka_headers_t>>()
            if (rd_kafka_message_headers(message, headers.ptr) !=
                RD_KAFKA_RESP_ERR_NO_ERROR
            ) {
                return@memScoped emptyList()
            }
            val all = headers.value ?: return@memScoped emptyList()
            (0 until rd_kafka_header_cnt(all).toInt()).map { index ->
                val name = alloc<CPointerVar<ByteVar>>()
                val value = alloc<COpaquePointerVar>()
                val size = alloc<size_tVar>()
                rd_kafka_header_get_all(all, index.convert(), name.ptr, value.ptr, size.ptr)
                RecordHeader(name.value?.toKString() ?: "", value.value?.readBytes(size.value.toInt()))
            }
        }

    override suspend fun close() {
        enter("close")
        serial.withLock {
            // rd_kafka_consumer_close leaves any group and joins librdkafka's threads; it waits. Leaving
            // revokes, so the rebalance callback, and a listener's onRevoked, run inside it.
            withContext(Dispatchers.IO) {
                rd_kafka_consumer_close(handle)
                rd_kafka_destroy(handle)
            }
            bridgeRef.dispose()
            rethrowFromCallback()
        }
    }

    /** A partition list for one call, with each entry's offset set, freed however the call ends. */
    private inline fun <T> withPartitionList(
        entries: List<Pair<TopicPartition, Long>>,
        anyPartition: Boolean = false,
        use: (CPointer<rd_kafka_topic_partition_list_t>) -> T,
    ): T {
        val list =
            rd_kafka_topic_partition_list_new(entries.size) ?: error("rd_kafka_topic_partition_list_new returned null")
        try {
            for ((partition, offset) in entries) {
                // A subscription names topics, not partitions: librdkafka's "unassigned", -1.
                val number = if (anyPartition) PARTITION_UNASSIGNED else partition.partition
                val entry =
                    rd_kafka_topic_partition_list_add(list, partition.topic, number)
                        ?: error("list add failed")
                entry.pointed.offset = offset
            }
            return use(list)
        } finally {
            rd_kafka_topic_partition_list_destroy(list)
        }
    }

    private companion object {
        const val ERRSTR = 512
        const val POLL_IDLE_MS = 2L
        const val DEFAULT_SOCKET_TIMEOUT_MS = 60_000

        /** The Java client's `max.poll.records` default, as the native arm's per-`poll` bound (contract §2). */
        const val MAX_RECORDS = 500

        // librdkafka's logical offsets. Defined in rdkafka.h as casts, which cinterop does not carry.
        const val OFFSET_BEGINNING = -2L
        const val OFFSET_END = -1L
        const val OFFSET_INVALID = -1001L
        const val PARTITION_UNASSIGNED = -1
    }
}

/** librdkafka's group metadata as the bytes it serialises to — same process, same build only (B-38). */
internal class NativeGroupMetadata(
    val serialized: ByteArray,
    override val groupId: String,
) : ConsumerGroupMetadata()

/** What the rebalance callback needs from the consumer that installed it (consumer-contract §2a). */
private class RebalanceBridge(
    /** librdkafka's bound for the one blocking call a callback may make, offsets for a time. */
    val requestTimeoutMs: Int,
) {
    var listener: RebalanceListener? = null

    /** Where a seek in a group put a partition, until a record from it is read (position, B-51). */
    val groupSeeks = mutableMapOf<TopicPartition, Long>()

    /** What the listener threw, kept here because a C callback cannot throw; the call it ran in throws it. */
    var failure: Throwable? = null
}

/** Set while a rebalance callback runs on this thread: a call to the consumer from there is refused. */
@kotlin.native.concurrent.ThreadLocal
private object Reentry {
    var inside = false
}

/**
 * librdkafka's rebalance callback, on the thread inside `rd_kafka_consumer_poll` or
 * `rd_kafka_consumer_close`. Once a callback is set, librdkafka no longer applies an assignment by itself,
 * so this does, in every case, whatever the listener did. `onRevoked` runs **before** the unassign, while
 * the partitions are still this member's, which is what makes a commit in it count. `onAssigned` runs
 * after the assign.
 */
private fun onRebalance(
    rk: CPointer<rd_kafka_t>?,
    err: rd_kafka_resp_err_t,
    partitions: CPointer<rd_kafka_topic_partition_list_t>?,
    opaque: COpaquePointer?,
) {
    val bridge = opaque?.asStableRef<RebalanceBridge>()?.get() ?: return
    val listener = bridge.listener
    val list = partitions?.let { toPartitions(it) }.orEmpty()
    val cooperative = rd_kafka_rebalance_protocol(rk)?.toKString() == "COOPERATIVE"
    when (err) {
        RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS -> {
            // The listener first, then the assignment: librdkafka refuses a seek right after an assign
            // ("Erroneous state", B-36), so a seek from onAssigned becomes the partition's starting offset in
            // the list assigned. No record can be fetched in between, which is what the contract promises.
            if (listener != null && list.isNotEmpty() && partitions != null) {
                val scope = AssignScope(rk!!, list, bridge.requestTimeoutMs)
                inside(bridge) { listener.onAssigned(list, scope) }
                for (index in 0 until partitions.pointed.cnt) {
                    val entry = partitions.pointed.elems!![index]
                    scope.sought[TopicPartition(entry.topic!!.toKString(), entry.partition)]?.let { entry.offset = it }
                }
                bridge.groupSeeks.putAll(scope.sought)
            }
            if (cooperative) {
                rd_kafka_incremental_assign(rk, partitions)?.let {
                    rd_kafka_error_destroy(it)
                }
            } else {
                rd_kafka_assign(rk, partitions)
            }
        }

        RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS -> {
            list.forEach { bridge.groupSeeks.remove(it) }
            try {
                if (listener != null && list.isNotEmpty()) {
                    val lost = rd_kafka_assignment_lost(rk) == 1
                    inside(bridge) {
                        if (lost) listener.onLost(list) else listener.onRevoked(list, NativeRebalanceScope(rk!!))
                    }
                }
            } finally {
                if (cooperative) {
                    rd_kafka_incremental_unassign(rk, partitions)?.let {
                        rd_kafka_error_destroy(it)
                    }
                } else {
                    rd_kafka_assign(rk, null)
                }
            }
        }

        // An error in place of an assignment: drop what is held, as librdkafka's own example does.
        else -> {
            rd_kafka_assign(rk, null)
        }
    }
}

private inline fun inside(
    bridge: RebalanceBridge,
    call: () -> Unit,
) {
    Reentry.inside = true
    try {
        call()
    } catch (failure: Throwable) {
        // Nothing may cross back into C. Kept, and thrown by the poll or close this ran inside.
        if (bridge.failure == null) bridge.failure = failure
    } finally {
        Reentry.inside = false
    }
}

private open class NativeRebalanceScope(
    protected val rk: CPointer<rd_kafka_t>,
) : RebalanceScope {
    override fun commit(offsets: Map<TopicPartition, Long>) {
        requireCommittable(offsets)
        if (offsets.isNotEmpty()) commitNow(rk, offsets)
    }

    override fun seek(
        partition: TopicPartition,
        to: SeekTo,
    ): Unit = throw IllegalStateException("seek from onRevoked: $partition is leaving this member")
}

/** The scope inside onAssigned: a seek is kept and becomes the partition's starting offset (B-51). */
private class AssignScope(
    rk: CPointer<rd_kafka_t>,
    private val arriving: List<TopicPartition>,
    private val requestTimeoutMs: Int,
) : NativeRebalanceScope(rk) {
    val sought = mutableMapOf<TopicPartition, Long>()

    override fun seek(
        partition: TopicPartition,
        to: SeekTo,
    ) {
        check(partition in arriving) { "seek: $partition is not among the partitions arriving" }
        sought[partition] = seekTarget(rk, partition, to, requestTimeoutMs)
    }
}

private fun requestTimeoutMsFor(properties: Map<String, String>): Int =
    properties["socket.timeout.ms"]?.toIntOrNull() ?: 60_000

/** Where a [SeekTo] points, as librdkafka takes it: an offset, or the logical beginning or end. */
private fun seekTarget(
    rk: CPointer<rd_kafka_t>,
    partition: TopicPartition,
    to: SeekTo,
    requestTimeoutMs: Int,
): Long =
    when (to) {
        SeekTo.Beginning -> LOGICAL_BEGINNING
        SeekTo.End -> LOGICAL_END
        is SeekTo.Offset -> to.offset
        is SeekTo.Timestamp -> offsetForTime(rk, partition, to.timestamp, requestTimeoutMs)
    }

/** `rd_kafka_offsets_for_times`: the offset of the first record at or after [timestamp], or the end. */
private fun offsetForTime(
    rk: CPointer<rd_kafka_t>,
    partition: TopicPartition,
    timestamp: Long,
    requestTimeoutMs: Int,
): Long {
    val list = rd_kafka_topic_partition_list_new(1) ?: error("rd_kafka_topic_partition_list_new returned null")
    try {
        rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)!!.pointed.offset = timestamp
        val err = rd_kafka_offsets_for_times(rk, list, requestTimeoutMs)
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw KafkaConsumeException(
                "offsets for time $timestamp on $partition: ${rd_kafka_err2str(err)?.toKString()}",
            )
        }
        val found = list.pointed.elems!![0].offset
        // -1 is librdkafka's "no record at or after that time": the end, as the contract says.
        return if (found < 0) LOGICAL_END else found
    } finally {
        rd_kafka_topic_partition_list_destroy(list)
    }
}

/** `rd_kafka_seek_partitions` of one partition in a group: the call's error and the partition's own are read. */
private fun seekPartitionsNow(
    rk: CPointer<rd_kafka_t>,
    partition: TopicPartition,
    offset: Long,
    requestTimeoutMs: Int,
) {
    val list = rd_kafka_topic_partition_list_new(1) ?: error("rd_kafka_topic_partition_list_new returned null")
    try {
        rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)!!.pointed.offset = offset
        rd_kafka_seek_partitions(rk, list, requestTimeoutMs)?.let { failure ->
            val why = rd_kafka_error_string(failure)?.toKString()
            rd_kafka_error_destroy(failure)
            throw KafkaConsumeException("seek $partition: $why")
        }
        val entry = list.pointed.elems!![0]
        if (entry.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw KafkaConsumeException("seek $partition: ${rd_kafka_err2str(entry.err)?.toKString()}")
        }
    } finally {
        rd_kafka_topic_partition_list_destroy(list)
    }
}

private const val LOGICAL_BEGINNING = -2L
private const val LOGICAL_END = -1L

private fun toPartitions(list: CPointer<rd_kafka_topic_partition_list_t>): List<TopicPartition> =
    (0 until list.pointed.cnt)
        .map { index ->
            val entry = list.pointed.elems!![index]
            TopicPartition(entry.topic!!.toKString(), entry.partition)
        }.sortedWith(PARTITION_ORDER)

/**
 * `rd_kafka_commit` of exactly these offsets, synchronously, with every partition's own error read as
 * well as the call's (B-48). Used by `commit(offsets)` and by the revocation scope, which runs inside the
 * call that holds the consumer and so must not go through it.
 */
private fun commitNow(
    rk: CPointer<rd_kafka_t>,
    offsets: Map<TopicPartition, Long>,
) {
    val list =
        rd_kafka_topic_partition_list_new(offsets.size) ?: error("rd_kafka_topic_partition_list_new returned null")
    try {
        for ((partition, offset) in offsets) {
            val entry =
                rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)
                    ?: error("list add failed")
            entry.pointed.offset = offset
        }
        val err = rd_kafka_commit(rk, list, 0)
        val refused =
            (0 until list.pointed.cnt).mapNotNull { index ->
                val entry = list.pointed.elems!![index]
                if (entry.err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                    null
                } else {
                    val why = rd_kafka_err2str(entry.err)?.toKString()
                    "${entry.topic?.toKString()}-${entry.partition}: $why"
                }
            }
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR || refused.isNotEmpty()) {
            throw KafkaConsumeException("commit: ${rd_kafka_err2str(err)?.toKString()} $refused")
        }
    } finally {
        rd_kafka_topic_partition_list_destroy(list)
    }
}
