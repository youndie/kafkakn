@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointed
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.MemScope
import kotlinx.cinterop.StableRef
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.refTo
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.toCPointer
import kotlinx.cinterop.toKString
import kotlinx.cinterop.toLong
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import platform.posix.size_tVar
import rdkafka.RD_KAFKA_CONF_OK
import rdkafka.RD_KAFKA_CONF_UNKNOWN
import rdkafka.RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION
import rdkafka.RD_KAFKA_RESP_ERR_NO_ERROR
import rdkafka.RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID
import rdkafka.RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
import rdkafka.RD_KAFKA_RESP_ERR__FATAL
import rdkafka.RD_KAFKA_RESP_ERR__FENCED
import rdkafka.RD_KAFKA_RESP_ERR__QUEUE_FULL
import rdkafka.rd_kafka_abort_transaction
import rdkafka.rd_kafka_begin_transaction
import rdkafka.rd_kafka_commit_transaction
import rdkafka.rd_kafka_conf
import rdkafka.rd_kafka_conf_get
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
import rdkafka.rd_kafka_conf_set_dr_msg_cb
import rdkafka.rd_kafka_conf_set_error_cb
import rdkafka.rd_kafka_conf_set_oauthbearer_token_refresh_cb
import rdkafka.rd_kafka_conf_set_opaque
import rdkafka.rd_kafka_conf_set_stats_cb
import rdkafka.rd_kafka_consumer_group_metadata_destroy
import rdkafka.rd_kafka_consumer_group_metadata_read
import rdkafka.rd_kafka_consumer_group_metadata_t
import rdkafka.rd_kafka_destroy
import rdkafka.rd_kafka_err2str
import rdkafka.rd_kafka_error_code
import rdkafka.rd_kafka_error_destroy
import rdkafka.rd_kafka_error_is_fatal
import rdkafka.rd_kafka_error_is_retriable
import rdkafka.rd_kafka_error_name
import rdkafka.rd_kafka_error_string
import rdkafka.rd_kafka_error_t
import rdkafka.rd_kafka_error_txn_requires_abort
import rdkafka.rd_kafka_fatal_error
import rdkafka.rd_kafka_flush
import rdkafka.rd_kafka_init_transactions
import rdkafka.rd_kafka_message_t
import rdkafka.rd_kafka_message_timestamp
import rdkafka.rd_kafka_metadata
import rdkafka.rd_kafka_metadata_destroy
import rdkafka.rd_kafka_metadata_t
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_outq_len
import rdkafka.rd_kafka_poll
import rdkafka.rd_kafka_produceva
import rdkafka.rd_kafka_resp_err_t
import rdkafka.rd_kafka_send_offsets_to_transaction
import rdkafka.rd_kafka_t
import rdkafka.rd_kafka_topic_destroy
import rdkafka.rd_kafka_topic_name
import rdkafka.rd_kafka_topic_new
import rdkafka.rd_kafka_topic_partition_list_add
import rdkafka.rd_kafka_topic_partition_list_destroy
import rdkafka.rd_kafka_topic_partition_list_new
import rdkafka.rd_kafka_type_t
import rdkafka.rd_kafka_vtype_t
import rdkafka.rd_kafka_vu_t
import kotlin.concurrent.AtomicLong
import kotlin.concurrent.AtomicReference

/**
 * The native arm: librdkafka through cinterop.
 *
 * The load-bearing part is not the produce call. It is the seam where a C callback arriving on a
 * thread librdkafka owns has to resume a Kotlin coroutine that has been suspended since before the
 * record was enqueued.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = NativeKafkaProducer(config)

/** The default is murmur2_random; a caller who names a partitioner keeps theirs. */
private fun partitionerFor(config: ProducerConfig): String = config.properties["partitioner"] ?: "murmur2_random"

/**
 * The idempotence the Java client would have for the same configuration, or `null` when the caller
 * named it — **and it is not simply "on"**.
 *
 * librdkafka defaults `enable.idempotence` to `false` and `kafka-clients` 4.3.1 to `true`, so a retried
 * record whose acknowledgement was lost could be written twice by this arm and once by the other
 * ([B-25](../../../../../../../docs/backlog/B-25-the-arms-disagree-on-idempotence.md)). The fix is to
 * take the reference arm's default — and its default is conditional, measured against the jar: it
 * turns itself **off, silently,** when the caller set `acks` to anything but all or `retries` to zero.
 * A native default that was only "on" would make librdkafka refuse `acks=1`, which the reference
 * accepts, and that is a configuration that works on the arm a caller runs locally and fails on the
 * one they ship.
 *
 * The third row of the Java client's behaviour — more than five requests in flight is refused even
 * with idempotence unset — needs nothing here: with the default on, librdkafka refuses it too.
 */
private fun idempotenceFor(config: ProducerConfig): String? {
    val properties = config.properties
    if ("enable.idempotence" in properties) return null
    // `request.required.acks` is librdkafka's own name for `acks`; either spelling can carry the value.
    val acks = properties["acks"] ?: properties["request.required.acks"]
    if (acks != null && acks != "all" && acks != "-1") return "false"
    val retries = properties["retries"] ?: properties["message.send.max.retries"]
    if (retries == "0") return "false"
    return "true"
}

// RD_KAFKA_PARTITION_UA and RD_KAFKA_MSG_F_COPY are preprocessor macros, so cinterop does not
// publish them. Spelled with the names they have in rdkafka.h so a reader can check them.
private const val RD_KAFKA_PARTITION_UA: Int = -1
private const val RD_KAFKA_MSG_F_COPY: Int = 0x2

/**
 * The continuations waiting for a delivery report, by the id handed to librdkafka as `msg_opaque`.
 *
 * It is a top-level atomic rather than a field because the callback is a `staticCFunction` and
 * cannot capture: the only thing it receives is the opaque, so the way back to the caller has to be
 * reachable from nowhere in particular. The map is replaced wholesale under compare-and-set, which
 * is enough for a registry written by producer threads and read by poll threads.
 */
private val waiting = AtomicReference<Map<Long, CompletableDeferred<RecordMetadata>>>(emptyMap())
private val nextId = AtomicLong(1)

/** How often a caller has had to wait for room. Read by the suite; not public API. */
internal val backpressureWaits = AtomicLong(0)

private fun park(
    id: Long,
    slot: CompletableDeferred<RecordMetadata>,
) {
    while (true) {
        val current = waiting.value
        if (waiting.compareAndSet(current, current + (id to slot))) return
    }
}

private fun unpark(id: Long): CompletableDeferred<RecordMetadata>? {
    while (true) {
        val current = waiting.value
        val found = current[id] ?: return null
        if (waiting.compareAndSet(current, current - id)) return found
    }
}

/**
 * A test affordance, and deliberately **not** an environment variable.
 *
 * "Zero crashes in the callback" is worth nothing until the suite has been shown noticing one, and
 * the callback is where a crash is easiest to miss: it runs on a thread librdkafka owns, outside any
 * `try` the caller wrote. So the suite sets this, calls `send`, and asserts the process dies.
 *
 * `internal`, so it is not public API, and set from nowhere but `linuxX64Test`. An env var would
 * have been a switch anyone could find and turn on in production.
 */
internal val crashInDeliveryCallback = AtomicReference(false)

/**
 * What librdkafka last complained about on a connection, as free text.
 *
 * **Without this a TLS failure is unrecognisable.** `rd_kafka_produce` only enqueues, so a record
 * bound for a broker whose certificate cannot be verified is queued, retried, and finally delivered
 * back as `Local: Message timed out` — the same words a closed port, a wrong address and a dead
 * broker produce. The reason it could not connect goes to the error callback and nowhere else, so
 * a producer that does not keep it cannot tell its caller why.
 *
 * Top-level and atomic for the same reason as the registry above: a `staticCFunction` captures
 * nothing.
 */
private val lastConnectionError = AtomicReference<String?>(null)

private val errorReport =
    staticCFunction<
        CPointer<rd_kafka_t>?,
        Int,
        CPointer<ByteVar>?,
        COpaquePointer?,
        Unit,
    > { _, code, reason, _ ->
        // `_ALL_BROKERS_DOWN` is skipped, and that is the difference between a usable message and a
        // useless one. It is a SUMMARY of other errors - librdkafka's own header calls it informational
        // and says not to treat it as fatal - and it arrives last, after the error that explains
        // anything. Measured: keeping the last error of any kind produced
        // `Local: All broker connections are down: 1/1 brokers are down` for a certificate that could
        // not be verified, a certificate that had expired, and a port with nothing on it alike.
        if (code == RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN) return@staticCFunction
        lastConnectionError.value = "${rd_kafka_err2str(code)?.toKString()}: ${reason?.toKString()}"
    }

/**
 * Invoked by librdkafka on one of its own threads, from inside `rd_kafka_poll`.
 *
 * Everything it touches is either the message it was handed or the atomic registry above; it
 * captures nothing, because a `staticCFunction` cannot.
 */
private val deliveryReport =
    staticCFunction<
        CPointer<rd_kafka_t>?,
        CPointer<rd_kafka_message_t>?,
        COpaquePointer?,
        Unit,
    > { _, message, _ ->
        val record = message?.pointed ?: return@staticCFunction
        // Unparked FIRST, and everything after it is inside a catch. Measured 2026-09-17: an exception
        // thrown on librdkafka's thread does NOT terminate the process and surfaces nowhere - it simply
        // leaves the continuation parked, and the caller stays suspended for ever. A hang is a worse
        // failure than a crash, because nothing reports it.
        val slot = unpark(record._private.toLong()) ?: return@staticCFunction
        try {
            if (crashInDeliveryCallback.value) {
                val nothing: String? = null
                nothing!!.length
            }
            val error = record.err
            val topic = rd_kafka_topic_name(record.rkt)?.toKString() ?: "<unknown topic>"
            if (error == RD_KAFKA_RESP_ERR_NO_ERROR) {
                slot.complete(
                    RecordMetadata(
                        topic = topic,
                        partition = record.partition,
                        offset = record.offset,
                        // What the broker kept: the record's own time on an ordinary topic, the
                        // broker's clock on a LogAppendTime one. librdkafka also reports which of the
                        // two it was; the Java client cannot, so that half stays here (B-28).
                        timestamp = rd_kafka_message_timestamp(message, null),
                    ),
                )
            } else {
                // The topic is spelled into the message deliberately. rd_kafka_err2str gives
                // "Broker: Unknown topic or partition" and names nothing, so a caller with several
                // topics in flight learns which one failed only if we say.
                //
                // The connection error is appended when there is one. `Local: Message timed out` is
                // what a certificate that cannot be verified looks like from here, and it is also what
                // a closed port looks like; the sentence that tells them apart arrived on the error
                // callback minutes earlier.
                val why = lastConnectionError.value?.let { "; last broker error: $it" } ?: ""
                slot.completeExceptionally(
                    KafkaProduceException("$topic: ${rd_kafka_err2str(error)?.toKString()}$why"),
                )
            }
        } catch (failure: Throwable) {
            // Whatever went wrong here, the caller is waiting. Resuming it with the failure is the only
            // outcome that is not a hang.
            slot.completeExceptionally(
                KafkaProduceException("delivery callback failed: ${failure::class.simpleName}: ${failure.message}"),
            )
        }
    }

/** A scope-lived pointer to [bytes]; an empty array still needs an address librdkafka can hold. */
private fun MemScope.bytes(bytes: ByteArray): CPointer<ByteVar> =
    if (bytes.isEmpty()) allocArray(1) else bytes.refTo(0).getPointer(this)

/** librdkafka could not describe a topic: no answer in time, or a topic the cluster does not have. */
public class KafkaMetadataException(
    message: String,
) : RuntimeException(message)

/** A failure reported by librdkafka for one record. */
public class KafkaProduceException(
    message: String,
) : RuntimeException(message)

internal class NativeKafkaProducer(
    private val config: ProducerConfig,
) : KafkaProducer {
    init {
        // BEFORE the handle below, because property initialisers run in source order and this one
        // has to answer before any C is touched. The rule it enforces is shared with the other arm
        // on purpose: a configuration refused on one arm only is a configuration the caller meets
        // for the first time on the platform they do not run locally.
        config.checkTlsKeys()
        config.checkSaslKeys()
    }

    /**
     * The OAUTHBEARER bridge (B-33), before the handle because the handle's configuration names it: its
     * own scope, since the pump's is declared after the handle, and a [StableRef] librdkafka hands back
     * to the refresh callback as the opaque.
     */
    private val oauthScope = CoroutineScope(Dispatchers.Default)

    /** The handle's opaque: the OAUTHBEARER bridge if there is one, and the latest statistics (B-41). */
    private val context =
        StableRef.create(HandleContext(config.oauthBearerTokenProvider?.let { OAuthBearerBridge(it, oauthScope) }))

    private val handle: CPointer<rd_kafka_t> =
        memScoped {
            val conf = rd_kafka_conf_new() ?: error("rd_kafka_conf_new returned null")
            val errstr = allocArray<ByteVar>(ERRSTR)
            // EVERYTHING the caller set goes on the global conf, topic-level properties included.
            // librdkafka accepts those here and applies them to the default topic configuration it
            // creates implicitly - which is the configuration `rd_kafka_topic_new(.., NULL)` then uses.
            //
            // The earlier shape built a fresh topic conf per topic, and that SILENTLY DROPPED every
            // topic-level property the caller had set: `message.timeout.ms`, `acks`, `compression.codec`
            // among them. It was found by a TLS test that hung for a minute where it had asked to fail
            // in twenty seconds, and it had been passing its own `acks=all` assertion all along because
            // librdkafka's default for acks happens to be -1 (research §2.8).
            val defaults =
                buildMap {
                    put("partitioner", partitionerFor(config))
                    idempotenceFor(config)?.let { put("enable.idempotence", it) }
                    // Statistics once a second, unless the caller chose: without them the native arm has
                    // no metrics at all - librdkafka's default interval is 0, off (B-41).
                    if ("statistics.interval.ms" !in config.properties) put("statistics.interval.ms", STATISTICS_MS)
                }
            (config.properties + defaults).forEach { (key, value) ->
                // librdkafka reports an unknown key here, so this arm refuses it at construction too -
                // the contract says an unusable configuration fails, and the earlier the better.
                // TWO REFUSALS, NOT ONE. librdkafka separates a name it does not know from a value it will
                // not take, and this used to report both as "unknown producer configuration" - so
                // `compression.type=brotli` sent the caller looking for a typo in a key they had spelled
                // correctly (B-26). librdkafka's own text also names the key by its canonical spelling,
                // `compression.codec`, which is not the one the caller wrote; the caller's key and value
                // go first, and librdkafka's sentence follows as the evidence.
                val result = rd_kafka_conf_set(conf, key, value, errstr, ERRSTR.convert())
                if (result == RD_KAFKA_CONF_UNKNOWN) {
                    throw IllegalArgumentException("unknown producer configuration: $key (${errstr.toKString()})")
                }
                if (result != RD_KAFKA_CONF_OK) {
                    throw IllegalArgumentException(
                        "producer configuration $key refuses the value '$value' (${errstr.toKString()})",
                    )
                }
            }
            rd_kafka_conf_set_dr_msg_cb(conf, deliveryReport)
            rd_kafka_conf_set_error_cb(conf, errorReport)
            // Callbacks that run inside rd_kafka_poll - the pump - find this producer's state here.
            rd_kafka_conf_set_opaque(conf, context.asCPointer())
            rd_kafka_conf_set_stats_cb(conf, statisticsReport)
            if (context.get().oauth != null) rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf, oauthBearerRefresh)
            rd_kafka_new(rd_kafka_type_t.RD_KAFKA_PRODUCER, conf, errstr, ERRSTR.convert())
                ?: error("rd_kafka_new failed: ${errstr.toKString()}")
        }

    init {
        // Cleared per producer, because [lastConnectionError] is one slot for the whole process: a
        // `staticCFunction` captures nothing, so there is nowhere per-producer for it to write.
        // Attributing it properly would mean handing each producer's identity through
        // `rd_kafka_conf_set_opaque` and keeping a second registry; two producers failing at once
        // is not a case this library has, and when it is, that is the shape of the fix.
        lastConnectionError.value = null
    }

    /**
     * What librdkafka itself settled on for [key], read off the handle rather than out of [config].
     *
     * For tests of the platform seam, and the difference matters: kafkakn adds keys the caller never
     * wrote (the partitioner, the idempotence default), and librdkafka adjusts others when one is set.
     * A test that read [config] back would be checking kafkakn's intention, not what the client does.
     */
    internal fun effectiveConfig(key: String): String? =
        memScoped {
            val conf = rd_kafka_conf(handle) ?: return@memScoped null
            val size = alloc<size_tVar>()
            size.value = CONFIG_VALUE_MAX.convert()
            val value = allocArray<ByteVar>(CONFIG_VALUE_MAX)
            if (rd_kafka_conf_get(conf, key, value, size.ptr) != RD_KAFKA_CONF_OK) null else value.toKString()
        }

    private val pump = CoroutineScope(Dispatchers.Default)

    init {
        // The delivery callback only runs inside rd_kafka_poll, so somebody has to call it. This
        // coroutine is that somebody, and it is why a caller's `send` can be resumed at all.
        pump.launch {
            while (isActive) {
                rd_kafka_poll(handle, 0)
                delay(POLL_IDLE_MS)
            }
        }
    }

    /**
     * The partitioner default, and this is not a detail.
     *
     * **librdkafka and the Java client do not agree by default.** librdkafka's default is
     * `consistent_random`, a CRC32 hash of the key; the Java producer uses murmur2. Both are
     * internally consistent, so neither implementation can notice on its own — the same key simply
     * goes somewhere different depending on which arm produced it, and every assertion each arm
     * makes about its own records still passes.
     *
     * librdkafka names the compatible option itself: `murmur2_random` is documented as
     * "functionally equivalent to the default partitioner in the Java Producer". So kafkakn defaults
     * to it, because one library that puts a key in two different places depending on the platform
     * is not one library.
     *
     * A caller who sets `partitioner` explicitly keeps their choice; the default is a default.
     */

    override suspend fun send(record: ProducerRecord): RecordMetadata {
        val id = nextId.addAndGet(1)
        val slot = CompletableDeferred<RecordMetadata>()

        // Parked BEFORE the record is enqueued, because the delivery report can arrive before this
        // function returns. A registry filled afterwards races with the very callback it is for.
        park(id, slot)
        try {
            enqueue(id, record)
        } catch (failure: Throwable) {
            unpark(id)
            throw failure
        }
        // Cancelling here stops the caller waiting. It does not recall a record librdkafka has
        // already accepted, and the contract does not pretend otherwise.
        return slot.await()
    }

    /**
     * Hands the record to librdkafka, **suspending** while its queue is full.
     *
     * `rd_kafka_produce` refuses with `QUEUE_FULL` when the queue is at
     * `queue.buffering.max.messages`. That refusal is backpressure, not an error: the caller is not
     * told to try again, and the thread is not held while we wait. Returning a failure the caller may
     * ignore is what lost 264 826 records of 1 000 000 in the measurement this project starts from.
     *
     * Cancelling while suspended here means the record was **never queued**, which is the one moment
     * at which cancellation is clean.
     */
    private suspend fun enqueue(
        id: Long,
        record: ProducerRecord,
    ) {
        var waited = 0L
        while (true) {
            val error = produceOnce(id, record)
            if (error == RD_KAFKA_RESP_ERR_NO_ERROR) return
            if (error != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                val said = "${record.topic}: ${rd_kafka_err2str(error)?.toKString()}"
                throw fencedOrNull(error, said) ?: KafkaProduceException(said)
            }
            backpressureWaits.addAndGet(1)
            // The poll is what drains the queue; the delay is what makes this a suspension rather
            // than a spin. Neither blocks the thread.
            rd_kafka_poll(handle, 0)
            delay(BACKPRESSURE_DELAY_MS)
            waited += BACKPRESSURE_DELAY_MS
            if (waited > BACKPRESSURE_LIMIT_MS) {
                throw KafkaProduceException(
                    "${record.topic}: the producer queue stayed full for ${waited}ms",
                )
            }
        }
    }

    /**
     * One attempt, through `rd_kafka_produceva` — the path that can carry headers.
     *
     * **`rd_kafka_producev` is variadic and unusable through cinterop** (research §1.5), and that was
     * taken for years as "librdkafka's header path needs a C shim". It does not:
     * `rd_kafka_produceva` takes the same tagged fields as an **array** of `rd_kafka_vu_t` and is an
     * ordinary function. This project therefore still contains no C of its own (research §2.10).
     *
     * `RD_KAFKA_VTYPE_HEADER` per header rather than one `VTYPE_HEADERS` list, and the difference is
     * ownership: with a `rd_kafka_headers_t` librdkafka takes it over **on success only**, so every
     * error path would have to remember to destroy it, and a queue-full retry loop is exactly where
     * that is forgotten. Mixing the two returns `_CONFLICT`, so this picks one.
     *
     * Everything the array points at lives in [memScoped] and outlives the call, which is enough:
     * `RD_KAFKA_MSG_F_COPY` means librdkafka has copied the payload by the time the call returns.
     */
    private fun produceOnce(
        id: Long,
        record: ProducerRecord,
    ): rd_kafka_resp_err_t =
        memScoped {
            val count =
                4 + (if (record.key != null) 1 else 0) + (if (record.partition != null) 1 else 0) +
                    (if (record.timestamp != null) 1 else 0) + record.headers.size
            val vus = allocArray<rd_kafka_vu_t>(count)
            var at = 0

            vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_TOPIC
            vus[at].u.cstr = record.topic.cstr.getPointer(this)
            at++

            vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_MSGFLAGS
            vus[at].u.i = RD_KAFKA_MSG_F_COPY
            at++

            // A null pointer is a null value, a tombstone (B-47); an empty array still gets an address, so
            // that librdkafka keeps it an empty value. The broker's own reader tells the two apart in
            // ci/b-47/run.sh.
            vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_VALUE
            vus[at].u.mem.ptr = record.value?.let { bytes(it) }
            vus[at].u.mem.size = (record.value?.size ?: 0).convert()
            at++

            record.key?.let { key ->
                vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_KEY
                vus[at].u.mem.ptr = bytes(key)
                vus[at].u.mem.size = key.size.convert()
                at++
            }

            // Only when the caller named one. Absent, librdkafka leaves the record unassigned and the
            // partitioner decides, which is `murmur2_random` for a keyed record (research §2.2); present,
            // the partitioner is not consulted at all - `int32_t`, the `i32` member of the union, which is
            // the type rdkafka.h declares for this tag.
            record.partition?.let { partition ->
                vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_PARTITION
                vus[at].u.i32 = partition
                at++
            }

            // `int64_t` milliseconds, the union's `i64`, as rdkafka.h declares for this tag. Absent,
            // librdkafka stamps the record with its own clock when it is enqueued.
            record.timestamp?.let { timestamp ->
                vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_TIMESTAMP
                vus[at].u.i64 = timestamp
                at++
            }

            // The way back to the suspended caller, and the only thing the delivery callback receives.
            vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_OPAQUE
            vus[at].u.ptr = id.toCPointer<CPointed>()
            at++

            record.headers.forEach { header ->
                vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_HEADER
                vus[at].u.header.name = header.name.cstr.getPointer(this)
                // A NULL value is -1, not 0. Kafka distinguishes a header with no value from one whose
                // value is empty, and a consumer can see the difference.
                vus[at].u.header.`val` = header.value?.let { bytes(it) }
                vus[at].u.header.size = (header.value?.size ?: -1).convert()
                at++
            }

            val failure =
                rd_kafka_produceva(handle, vus, count.convert())
                    ?: return@memScoped RD_KAFKA_RESP_ERR_NO_ERROR
            val code = rd_kafka_error_code(failure)
            rd_kafka_error_destroy(failure)
            code
        }

    /**
     * On `Dispatchers.IO`: `rd_kafka_metadata` blocks for up to its timeout, and on the caller's
     * dispatcher that held a single-lane dispatcher for 5 s against a broker that was not there —
     * measured before this was moved (`TopicMetadataTest`). Cancelling the caller stops it waiting;
     * it does not interrupt librdkafka, which returns when its timeout does.
     */
    override suspend fun partitionsFor(topic: String): List<PartitionInfo> =
        withContext(Dispatchers.IO) { describe(topic) }

    /**
     * `rd_kafka_metadata` for one topic, and the blocking call this function exists to wrap.
     *
     * The timeout is the effective `socket.timeout.ms` — librdkafka's "default timeout for network
     * requests", and this is one. The Java client bounds the same question by `max.block.ms`, a key
     * that exists only there.
     */
    private fun describe(topic: String): List<PartitionInfo> =
        memScoped {
            val timeout = effectiveConfig("socket.timeout.ms")?.toIntOrNull() ?: DEFAULT_SOCKET_TIMEOUT_MS
            val rkt = rd_kafka_topic_new(handle, topic, null) ?: error("rd_kafka_topic_new returned null for $topic")
            try {
                val described = alloc<CPointerVar<rd_kafka_metadata_t>>()
                val err = rd_kafka_metadata(handle, 0, rkt, described.ptr, timeout)
                if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    throw KafkaMetadataException("partitionsFor($topic): ${rd_kafka_err2str(err)?.toKString()}")
                }
                val metadata = described.value ?: error("rd_kafka_metadata succeeded and described nothing")
                try {
                    partitionsOf(topic, metadata.pointed)
                } finally {
                    rd_kafka_metadata_destroy(metadata)
                }
            } finally {
                rd_kafka_topic_destroy(rkt)
            }
        }

    private fun partitionsOf(
        topic: String,
        metadata: rd_kafka_metadata_t,
    ): List<PartitionInfo> {
        val described =
            (0 until metadata.topic_cnt).map { metadata.topics!![it] }.singleOrNull { it.topic?.toKString() == topic }
                ?: throw KafkaMetadataException(
                    "partitionsFor($topic): the cluster's answer did not describe the topic",
                )
        // A topic the broker does not have comes back as a described topic carrying an error, not as
        // a failed call - the error has to be read here or an unknown topic is an empty list.
        if (described.err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            throw KafkaMetadataException("partitionsFor($topic): ${rd_kafka_err2str(described.err)?.toKString()}")
        }
        return (0 until described.partition_cnt)
            .map { index ->
                val partition = described.partitions!![index]
                PartitionInfo(
                    topic = topic,
                    partition = partition.id,
                    leader = partition.leader.takeIf { it >= 0 },
                    replicas = (0 until partition.replica_cnt).map { partition.replicas!![it] },
                    inSyncReplicas = (0 until partition.isr_cnt).map { partition.isrs!![it] },
                )
            }.sortedBy { it.partition }
    }

    // The rd_kafka_*_transaction family. `-1` for every timeout, as librdkafka's header asks: for
    // init it means twice `transaction.timeout.ms`, for commit and abort the transaction's remaining
    // time, and the header warns that any other value risks "internal state desynchronization" when
    // an underlying request fails. Init, commit and abort block for up to that long, so they wait on
    // `Dispatchers.IO`; begin is local and returns at once.
    override suspend fun initTransactions() =
        withContext(Dispatchers.IO) {
            transactional("initTransactions") { rd_kafka_init_transactions(handle, -1) }
        }

    override suspend fun beginTransaction() = transactional("beginTransaction") { rd_kafka_begin_transaction(handle) }

    override suspend fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, Long>,
        group: ConsumerGroupMetadata,
    ) {
        val bytes =
            (group as? NativeGroupMetadata)?.serialized
                ?: throw IllegalArgumentException("group metadata from another arm or another library: $group")
        // Blocks until the offsets are in the transaction; `-1`, as for the other transactional calls.
        withContext(Dispatchers.IO) {
            memScoped {
                val metadata = alloc<CPointerVar<rd_kafka_consumer_group_metadata_t>>()
                val unreadable =
                    rd_kafka_consumer_group_metadata_read(metadata.ptr, bytes.refTo(0), bytes.size.convert())
                unreadable?.let { error ->
                    val said = rd_kafka_error_string(error)?.toKString()
                    rd_kafka_error_destroy(error)
                    throw KafkaProduceException("sendOffsetsToTransaction: group metadata unreadable: $said")
                }
                val list =
                    rd_kafka_topic_partition_list_new(offsets.size)
                        ?: error("rd_kafka_topic_partition_list_new returned null")
                try {
                    for ((partition, next) in offsets) {
                        rd_kafka_topic_partition_list_add(list, partition.topic, partition.partition)!!.pointed.offset =
                            next
                    }
                    transactional("sendOffsetsToTransaction") {
                        rd_kafka_send_offsets_to_transaction(handle, list, metadata.value, -1)
                    }
                } finally {
                    rd_kafka_topic_partition_list_destroy(list)
                    rd_kafka_consumer_group_metadata_destroy(metadata.value)
                }
            }
        }
    }

    override suspend fun commitTransaction() =
        withContext(Dispatchers.IO) {
            transactional("commitTransaction") { rd_kafka_commit_transaction(handle, -1) }
        }

    override suspend fun abortTransaction() =
        withContext(Dispatchers.IO) {
            transactional("abortTransaction") { rd_kafka_abort_transaction(handle, -1) }
        }

    /**
     * librdkafka's fencing, as the one exception kafkakn throws for it on both arms (B-30).
     *
     * Two roads lead here. A transactional call reports `_FENCED` itself; anything after that meets
     * the producer's fatal state as `_FATAL`, and which fatal error it was is asked of
     * `rd_kafka_fatal_error`. librdkafka's own sentence stays in the message.
     */
    private fun fencedOrNull(
        code: rd_kafka_resp_err_t,
        said: String,
    ): ProducerFencedException? {
        val fenced =
            code == RD_KAFKA_RESP_ERR__FENCED ||
                (
                    code == RD_KAFKA_RESP_ERR__FATAL && rd_kafka_fatal_error(
                        handle,
                        null,
                        0u,
                    ) == RD_KAFKA_RESP_ERR__FENCED
                )
        return if (fenced) {
            ProducerFencedException(
                "fenced by a newer producer with the same transactional.id: $said",
            )
        } else {
            null
        }
    }

    /**
     * Offsets tied to a membership the group has moved past (B-71), as the one exception both arms throw for it:
     * `ILLEGAL_GENERATION` when the group rebalanced since the metadata was taken, `UNKNOWN_MEMBER_ID` when the
     * member has left. librdkafka marks both abortable, and so does the contract.
     */
    private fun staleOrNull(
        code: rd_kafka_resp_err_t,
        said: String,
    ): StaleGroupMetadataException? =
        if (code == RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION || code == RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID) {
            StaleGroupMetadataException(said)
        } else {
            null
        }

    /** Turns a returned `rd_kafka_error_t` into an exception, and frees it. */
    private fun transactional(
        what: String,
        call: () -> CPointer<rd_kafka_error_t>?,
    ) {
        val error = call() ?: return
        try {
            val code = rd_kafka_error_code(error)
            val name = rd_kafka_error_name(error)?.toKString()
            val text = rd_kafka_error_string(error)?.toKString()
            val flags =
                listOfNotNull(
                    "fatal".takeIf { rd_kafka_error_is_fatal(error) != 0 },
                    "retriable".takeIf { rd_kafka_error_is_retriable(error) != 0 },
                    "abortable".takeIf { rd_kafka_error_txn_requires_abort(error) != 0 },
                )
            val said = "$what: $name ($code): $text${if (flags.isEmpty()) "" else " $flags"}"
            throw fencedOrNull(code, said) ?: staleOrNull(code, said) ?: KafkaProduceException(said)
        } finally {
            rd_kafka_error_destroy(error)
        }
    }

    /** From the latest statistics document librdkafka emitted, parsed now (B-41). */
    override suspend fun metrics(): ProducerMetrics = metricsFrom(context.get().statistics.value)

    override suspend fun flush() {
        // rd_kafka_flush returns an ERROR CODE, not a count. Reading it as "how many are left" is
        // how a sibling measurement printed -185, which is a timeout wearing a quantity's clothes.
        // The count is rd_kafka_outq_len, and that is what completion means here.
        //
        // On Dispatchers.IO (B-43). rd_kafka_flush waits for as long as records are outstanding, up to
        // its timeout, on whatever thread calls it: measured, 5.5 s of a held single-lane dispatcher
        // while records waited on a broker that was not there - research §2.13 had said this arm never
        // blocks. It is not dropped in favour of the loop below: for the length of the call it makes
        // `linger.ms` count as zero (rk_flushing in rdkafka.c), which is what "flush" means to a caller
        // with a long linger, and polling alone would not do it.
        withContext(Dispatchers.IO) { rd_kafka_flush(handle, FLUSH_MS) }
        while (rd_kafka_outq_len(handle) > 0) {
            rd_kafka_poll(handle, 0)
            delay(POLL_IDLE_MS)
        }
    }

    override suspend fun close() {
        flush()
        pump.cancel()
        oauthScope.cancel()
        rd_kafka_destroy(handle)
        // After the handle: librdkafka may call the refresh callback until it is destroyed.
        context.dispose()
    }

    private companion object {
        const val ERRSTR = 512

        /** Longer than any value librdkafka keeps for a single key; a longer one reads as absent. */
        const val CONFIG_VALUE_MAX = 512
        const val FLUSH_MS = 30_000
        const val STATISTICS_MS = "1000"

        /** librdkafka's own default for `socket.timeout.ms`, for a value that somehow reads as absent. */
        const val DEFAULT_SOCKET_TIMEOUT_MS = 60_000
        const val POLL_IDLE_MS = 2L
        const val BACKPRESSURE_DELAY_MS = 1L

        // Not a retry budget the caller can ignore: a queue that never drains is a broken producer,
        // and hanging for ever would be worse than saying so.
        const val BACKPRESSURE_LIMIT_MS = 120_000L
    }
}
