package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord as ApacheRecord
import org.apache.kafka.common.header.internals.RecordHeader as ApacheHeader

/**
 * The JVM arm — and therefore the oracle.
 *
 * It **delegates**. Every line of our own logic here is a line the oracle no longer vouches for:
 * the value of this arm is that it is the reference implementation, not that it is ours.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = JvmKafkaProducer(config)

/**
 * The one place this arm is allowed to not be a plain delegate.
 *
 * The contract spells the certificate authority librdkafka's way — `ssl.ca.location`, a path — and
 * the Java client has no such key: it wants a trust store. Kafka 4.x reads a PEM trust store
 * directly, so the translation is two entries and no file conversion.
 *
 * **The alternative was to make the caller write both.** A configuration that must be spelled
 * differently per platform is a configuration the caller gets wrong on the arm they do not run
 * locally, and the whole claim of this library is one surface over two implementations
 * ([feature-secure-connection](../../../../../../../docs/features/feature-secure-connection.md)).
 *
 * A caller who sets `ssl.truststore.location` themselves is left alone: theirs is the Java client's
 * own key and it travels untouched.
 */
internal fun translateForJava(properties: Map<String, String>): Map<String, String> {
    val ca = properties["ssl.ca.location"] ?: return properties
    return properties - "ssl.ca.location" +
        mapOf(
            "ssl.truststore.location" to ca,
            "ssl.truststore.type" to "PEM",
        )
}

internal class JvmKafkaProducer(
    config: ProducerConfig,
) : KafkaProducer {
    private val properties = translateForJava(config.properties)

    init {
        // A key nobody honours fails HERE, not silently. kafka-clients logs unknown configuration at
        // WARN and carries on, which is the shape this project refuses: an option accepted and
        // dropped behaves exactly like one that worked, until it matters.
        //
        // The known set comes from the client itself rather than from a list maintained here - a
        // hand-written list beside a growing set goes stale on the first Kafka release.
        val known = ApacheProducerConfig.configNames()
        val unknown = properties.keys.filterNot { it in known }.sorted()
        require(unknown.isEmpty()) {
            "unknown producer configuration: ${unknown.joinToString()} " +
                "(kafka-clients knows ${known.size} keys; nothing here is silently ignored)"
        }
    }

    private val delegate =
        org.apache.kafka.clients.producer.KafkaProducer<ByteArray, ByteArray>(
            Properties().apply {
                properties.forEach { (key, value) -> setProperty(key, value) }
            },
            ByteArraySerializer(),
            ByteArraySerializer(),
        )

    /**
     * Bridges the client's callback into a suspension.
     *
     * No thread is blocked: `send` returns immediately and the continuation resumes on the client's
     * I/O thread when the broker acknowledges. Cancelling the coroutine stops the caller waiting; it
     * does **not** recall a record the client has already accepted, and nothing here pretends
     * otherwise.
     */
    override suspend fun send(record: ProducerRecord): RecordMetadata =
        // `Dispatchers.IO`, and this is the whole of what "suspends" means on this arm.
        //
        // `kafka-clients`' `send` WAITS inside the client before it returns: for metadata it does
        // not have, and for room in the record accumulator up to `max.block.ms`. Called straight
        // from a coroutine it holds that coroutine's thread - measured at 6 019 ms of silence on a
        // single-threaded dispatcher while three records waited on metadata, with a coroutine asking
        // for the thread every 2 ms and getting nothing (`JvmDispatcherSeamTest`).
        //
        // So the wait happens on a thread that exists for waiting. The caller's dispatcher stays
        // free, which is what the contract's "suspends" promises and what librdkafka's arm does by
        // never blocking at all.
        withContext(Dispatchers.IO) {
            suspendCancellableCoroutine { continuation ->
                delegate.send(record.toApache()) { metadata, failure ->
                    when {
                        failure != null -> {
                            continuation.resumeWithException(failure)
                        }

                        else -> {
                            continuation.resume(
                                RecordMetadata(metadata.topic(), metadata.partition(), metadata.offset()),
                            )
                        }
                    }
                }
            }
        }

    /**
     * The five-argument constructor, because the shorter ones cannot carry headers.
     *
     * `partition` and `timestamp` are null so the client decides both, which is what the
     * three-argument form did. The headers are handed over in order and duplicates are kept: the
     * Java client stores an ordered list too, so nothing has to be reconciled here.
     */
    private fun ProducerRecord.toApache(): ApacheRecord<ByteArray, ByteArray> =
        ApacheRecord(
            topic,
            null,
            null,
            key,
            value,
            headers.map { ApacheHeader(it.name, it.value) },
        )

    override suspend fun flush() {
        // Blocking too, and for longer: it waits for every record in flight.
        withContext(Dispatchers.IO) { delegate.flush() }
    }

    override suspend fun close() {
        // `close` flushes first, so it inherits the same wait.
        withContext(Dispatchers.IO) { delegate.close() }
    }
}
