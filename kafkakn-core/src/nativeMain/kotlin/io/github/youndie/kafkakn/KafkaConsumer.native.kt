@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointerVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.readBytes
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
import rdkafka.RD_KAFKA_RESP_ERR__NO_OFFSET
import rdkafka.RD_KAFKA_RESP_ERR__PARTITION_EOF
import rdkafka.rd_kafka_assign
import rdkafka.rd_kafka_assignment
import rdkafka.rd_kafka_commit
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
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
import rdkafka.rd_kafka_mem_free
import rdkafka.rd_kafka_message_destroy
import rdkafka.rd_kafka_message_headers
import rdkafka.rd_kafka_message_t
import rdkafka.rd_kafka_message_timestamp
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_offsets_for_times
import rdkafka.rd_kafka_poll_set_consumer
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

    override suspend fun assign(partitions: List<TopicPartition>) {
        serial.withLock {
            assigned = partitions
            positions.clear()
            reassign()
        }
    }

    /** Whether [subscribe] was the last of the two ways to get partitions. */
    private var subscribed = false

    override suspend fun subscribe(topics: List<String>) {
        requireGroup(namesAGroup, "subscribe")
        serial.withLock {
            // No rebalance callback: librdkafka then assigns and revokes by itself, eager or
            // incremental as the group's protocol requires, and with auto-commit off a revoked
            // partition resumes elsewhere from its last commit - the at-least-once the contract promises.
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
        requireGroup(namesAGroup, "commit")
        requireCommittable(offsets)
        if (offsets.isEmpty()) return
        serial.withLock {
            withPartitionList(offsets.entries.map { (partition, offset) -> partition to offset }) { list ->
                // Synchronous, as commit() is. With a list, librdkafka commits exactly these offsets and
                // reports an error per partition as well as for the call, so both are read.
                val err = withContext(Dispatchers.IO) { rd_kafka_commit(handle, list, 0) }
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
            }
        }
    }

    /**
     * librdkafka's group metadata, serialised: `rd_kafka_consumer_group_metadata_write` exists "for
     * client binding use", and bytes need no lifetime managed across the consumer and the producer. The
     * producer reads them back for the one call that needs the object (B-38).
     */
    override suspend fun groupMetadata(): ConsumerGroupMetadata {
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

    override suspend fun assignment(): List<TopicPartition> =
        serial.withLock {
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
        check(!subscribed) { "seek is refused under a subscription, on both arms: the group decides positions" }
        serial.withLock {
            require(partition in assigned) { "seek: $partition is not assigned (assigned: $assigned)" }
            val offset =
                when (to) {
                    SeekTo.Beginning -> OFFSET_BEGINNING

                    SeekTo.End -> OFFSET_END

                    is SeekTo.Offset -> to.offset

                    // A blocking call to the broker, on a thread that exists for it.
                    is SeekTo.Timestamp -> withContext(Dispatchers.IO) { offsetForTime(partition, to.timestamp) }
                }
            positions[partition] = offset
            reassign()
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

    /** `rd_kafka_offsets_for_times`: the offset of the first record at or after [timestamp], or the end. */
    private fun offsetForTime(
        partition: TopicPartition,
        timestamp: Long,
    ): Long =
        withPartitionList(listOf(partition to timestamp)) { list ->
            val err = rd_kafka_offsets_for_times(handle, list, requestTimeoutMs)
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                throw KafkaConsumeException(
                    "offsets for time $timestamp on $partition: ${rd_kafka_err2str(err)?.toKString()}",
                )
            }
            val found = list.pointed.elems!![0].offset
            // -1 is librdkafka's "no record at or after that time" — the end, as the contract says.
            if (found < 0) OFFSET_END else found
        }

    override suspend fun poll(timeout: Duration): List<ConsumerRecord> = serial.withLock { drain(timeout) }

    /** Drains what is here, up to the contract's bound; if nothing is, waits by `delay`, never in C. */
    private suspend fun drain(timeout: Duration): List<ConsumerRecord> {
        val started = TimeSource.Monotonic.markNow()
        val records = mutableListOf<ConsumerRecord>()
        while (true) {
            while (records.size < MAX_RECORDS) {
                val message = rd_kafka_consumer_poll(handle, 0) ?: break
                try {
                    read(message)?.let { record ->
                        records += record
                        positions[TopicPartition(record.topic, record.partition)] = record.offset + 1
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
        serial.withLock {
            // rd_kafka_consumer_close leaves any group and joins librdkafka's threads; it waits.
            withContext(Dispatchers.IO) {
                rd_kafka_consumer_close(handle)
                rd_kafka_destroy(handle)
            }
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
