@file:OptIn(ExperimentalForeignApi::class)

package io.github.youndie.kafkakn

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.CPointed
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
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
import rdkafka.RD_KAFKA_RESP_ERR__QUEUE_FULL
import rdkafka.rd_kafka_conf_new
import rdkafka.rd_kafka_conf_set
import rdkafka.rd_kafka_conf_set_dr_msg_cb
import rdkafka.rd_kafka_destroy
import rdkafka.rd_kafka_err2str
import rdkafka.rd_kafka_flush
import rdkafka.rd_kafka_last_error
import rdkafka.rd_kafka_message_t
import rdkafka.rd_kafka_new
import rdkafka.rd_kafka_outq_len
import rdkafka.rd_kafka_poll
import rdkafka.rd_kafka_produce
import rdkafka.rd_kafka_t
import rdkafka.rd_kafka_topic_destroy
import rdkafka.rd_kafka_topic_name
import rdkafka.rd_kafka_topic_conf_new
import rdkafka.rd_kafka_topic_conf_set
import rdkafka.rd_kafka_topic_new
import rdkafka.rd_kafka_type_t

/**
 * The native arm: librdkafka through cinterop.
 *
 * The load-bearing part is not the produce call. It is the seam where a C callback arriving on a
 * thread librdkafka owns has to resume a Kotlin coroutine that has been suspended since before the
 * record was enqueued.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = NativeKafkaProducer(config)

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
            slot.completeExceptionally(
                KafkaProduceException("$topic: ${rd_kafka_err2str(error)?.toKString()}"),
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

/** A failure reported by librdkafka for one record. */
public class KafkaProduceException(message: String) : RuntimeException(message)

internal class NativeKafkaProducer(private val config: ProducerConfig) : KafkaProducer {

    private val handle: CPointer<rd_kafka_t> = memScoped {
        val conf = rd_kafka_conf_new() ?: error("rd_kafka_conf_new returned null")
        val errstr = allocArray<ByteVar>(ERRSTR)
        // `partitioner` is a TOPIC property in librdkafka, not a global one; it is applied in
        // topicHandle and would be rejected here.
        config.properties.filterKeys { it != "partitioner" }.forEach { (key, value) ->
            // librdkafka reports an unknown key here, so this arm refuses it at construction too -
            // the contract says an unusable configuration fails, and the earlier the better.
            if (rd_kafka_conf_set(conf, key, value, errstr, ERRSTR.convert()) != RD_KAFKA_CONF_OK) {
                throw IllegalArgumentException("unknown producer configuration: $key (${errstr.toKString()})")
            }
        }
        rd_kafka_conf_set_dr_msg_cb(conf, deliveryReport)
        rd_kafka_new(rd_kafka_type_t.RD_KAFKA_PRODUCER, conf, errstr, ERRSTR.convert())
            ?: error("rd_kafka_new failed: ${errstr.toKString()}")
    }

    private val topics = mutableMapOf<String, CPointer<rdkafka.rd_kafka_topic_t>>()
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
     * The partitioner is set here, and this is not a detail.
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
    private fun topicHandle(name: String) = topics.getOrPut(name) {
        val topicConf = rd_kafka_topic_conf_new() ?: error("rd_kafka_topic_conf_new returned null")
        memScoped {
            val errstr = allocArray<ByteVar>(ERRSTR)
            val partitioner = config.properties["partitioner"] ?: "murmur2_random"
            if (rd_kafka_topic_conf_set(topicConf, "partitioner", partitioner, errstr, ERRSTR.convert())
                != RD_KAFKA_CONF_OK
            ) {
                throw IllegalArgumentException("partitioner=$partitioner rejected: ${errstr.toKString()}")
            }
        }
        rd_kafka_topic_new(handle, name, topicConf) ?: error("rd_kafka_topic_new failed for $name")
    }

    override suspend fun send(record: ProducerRecord): RecordMetadata {
        val id = nextId.addAndGet(1)
        val topic = topicHandle(record.topic)
        val slot = CompletableDeferred<RecordMetadata>()

        // Parked BEFORE the record is enqueued, because the delivery report can arrive before this
        // function returns. A registry filled afterwards races with the very callback it is for.
        park(id, slot)
        try {
            enqueue(id, topic, record)
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
    private suspend fun enqueue(id: Long, topic: CPointer<rdkafka.rd_kafka_topic_t>, record: ProducerRecord) {
        var waited = 0L
        while (true) {
            val rc = rd_kafka_produce(
                topic,
                RD_KAFKA_PARTITION_UA,
                RD_KAFKA_MSG_F_COPY,
                record.value.refTo(0),
                record.value.size.convert(),
                record.key?.refTo(0),
                (record.key?.size ?: 0).convert(),
                id.toCPointer<CPointed>(),
            )
            if (rc != -1) return

            val error = rd_kafka_last_error()
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
        topics.values.forEach { rd_kafka_topic_destroy(it) }
        topics.clear()
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
