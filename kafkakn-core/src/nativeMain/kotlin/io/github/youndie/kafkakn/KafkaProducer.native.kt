@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointed
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.cstr
import kotlinx.cinterop.get
import kotlinx.cinterop.convert
import kotlinx.cinterop.MemScope
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.refTo
import kotlinx.cinterop.staticCFunction
import kotlinx.cinterop.toCPointer
import kotlinx.cinterop.toKString
import kotlinx.cinterop.toLong
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.concurrent.AtomicLong
import kotlin.concurrent.AtomicReference
import rdkafka.RD_KAFKA_CONF_OK
import rdkafka.RD_KAFKA_RESP_ERR_NO_ERROR
import rdkafka.RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
import rdkafka.RD_KAFKA_RESP_ERR__QUEUE_FULL
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
import rdkafka.rd_kafka_conf_set_dr_msg_cb
import rdkafka.rd_kafka_conf_set_error_cb
import rdkafka.rd_kafka_destroy
import rdkafka.rd_kafka_err2str
import rdkafka.rd_kafka_flush
import rdkafka.rd_kafka_message_t
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_outq_len
import rdkafka.rd_kafka_poll
import rdkafka.rd_kafka_error_code
import rdkafka.rd_kafka_error_destroy
import rdkafka.rd_kafka_produceva
import rdkafka.rd_kafka_resp_err_t
import rdkafka.rd_kafka_vtype_t
import rdkafka.rd_kafka_vu_t
import rdkafka.rd_kafka_t
import rdkafka.rd_kafka_topic_name
import rdkafka.rd_kafka_type_t

/**
 * The native arm: librdkafka through cinterop.
 *
 * The load-bearing part is not the produce call. It is the seam where a C callback arriving on a
 * thread librdkafka owns has to resume a Kotlin coroutine that has been suspended since before the
 * record was enqueued.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = NativeKafkaProducer(config)

/** The default is murmur2_random; a caller who names a partitioner keeps theirs. */
private fun partitionerFor(config: ProducerConfig): String =
    config.properties["partitioner"] ?: "murmur2_random"

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

private fun park(id: Long, slot: CompletableDeferred<RecordMetadata>) {
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
 * Invoked by librdkafka on one of its own threads, from inside `rd_kafka_poll`.
 *
 * Everything it touches is either the message it was handed or the atomic registry above; it
 * captures nothing, because a `staticCFunction` cannot.
 */
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

private val errorReport = staticCFunction<
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

private val deliveryReport = staticCFunction<
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
                RecordMetadata(topic = topic, partition = record.partition, offset = record.offset),
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

/** A failure reported by librdkafka for one record. */
public class KafkaProduceException(message: String) : RuntimeException(message)

internal class NativeKafkaProducer(private val config: ProducerConfig) : KafkaProducer {

    private val handle: CPointer<rd_kafka_t> = memScoped {
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
        (config.properties + ("partitioner" to partitionerFor(config))).forEach { (key, value) ->
            // librdkafka reports an unknown key here, so this arm refuses it at construction too -
            // the contract says an unusable configuration fails, and the earlier the better.
            if (rd_kafka_conf_set(conf, key, value, errstr, ERRSTR.convert()) != RD_KAFKA_CONF_OK) {
                throw IllegalArgumentException("unknown producer configuration: $key (${errstr.toKString()})")
            }
        }
        rd_kafka_conf_set_dr_msg_cb(conf, deliveryReport)
        rd_kafka_conf_set_error_cb(conf, errorReport)
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
    private suspend fun enqueue(id: Long, record: ProducerRecord) {
        var waited = 0L
        while (true) {
            val error = produceOnce(id, record)
            if (error == RD_KAFKA_RESP_ERR_NO_ERROR) return
            if (error != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                throw KafkaProduceException(
                    "${record.topic}: ${rd_kafka_err2str(error)?.toKString()}",
                )
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
    private fun produceOnce(id: Long, record: ProducerRecord): rd_kafka_resp_err_t = memScoped {
        val count = 4 + (if (record.key != null) 1 else 0) + record.headers.size
        val vus = allocArray<rd_kafka_vu_t>(count)
        var at = 0

        vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_TOPIC
        vus[at].u.cstr = record.topic.cstr.getPointer(this)
        at++

        vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_MSGFLAGS
        vus[at].u.i = RD_KAFKA_MSG_F_COPY
        at++

        vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_VALUE
        vus[at].u.mem.ptr = bytes(record.value)
        vus[at].u.mem.size = record.value.size.convert()
        at++

        record.key?.let { key ->
            vus[at].vtype = rd_kafka_vtype_t.RD_KAFKA_VTYPE_KEY
            vus[at].u.mem.ptr = bytes(key)
            vus[at].u.mem.size = key.size.convert()
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

        val failure = rd_kafka_produceva(handle, vus, count.convert())
            ?: return@memScoped RD_KAFKA_RESP_ERR_NO_ERROR
        val code = rd_kafka_error_code(failure)
        rd_kafka_error_destroy(failure)
        code
    }

    override suspend fun flush() {
        // rd_kafka_flush returns an ERROR CODE, not a count. Reading it as "how many are left" is
        // how a sibling measurement printed -185, which is a timeout wearing a quantity's clothes.
        // The count is rd_kafka_outq_len, and that is what completion means here.
        rd_kafka_flush(handle, FLUSH_MS)
        while (rd_kafka_outq_len(handle) > 0) {
            rd_kafka_poll(handle, 0)
            delay(POLL_IDLE_MS)
        }
    }

    override suspend fun close() {
        flush()
        pump.cancel()
        rd_kafka_destroy(handle)
    }

    private companion object {
        const val ERRSTR = 512
        const val FLUSH_MS = 30_000
        const val POLL_IDLE_MS = 2L
        const val BACKPRESSURE_DELAY_MS = 1L
        // Not a retry budget the caller can ignore: a queue that never drains is a broken producer,
        // and hanging for ever would be worse than saying so.
        const val BACKPRESSURE_LIMIT_MS = 120_000L
    }
}
