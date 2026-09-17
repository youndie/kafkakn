package io.github.youndie.kafkakn

import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.producer.ProducerConfig as ApacheProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord as ApacheRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * The JVM arm — and therefore the oracle.
 *
 * It **delegates**. Every line of our own logic here is a line the oracle no longer vouches for:
 * the value of this arm is that it is the reference implementation, not that it is ours.
 */
public actual fun kafkaProducer(config: ProducerConfig): KafkaProducer = JvmKafkaProducer(config)

internal class JvmKafkaProducer(config: ProducerConfig) : KafkaProducer {

    init {
        // A key nobody honours fails HERE, not silently. kafka-clients logs unknown configuration at
        // WARN and carries on, which is the shape this project refuses: an option accepted and
        // dropped behaves exactly like one that worked, until it matters.
        //
        // The known set comes from the client itself rather than from a list maintained here - a
        // hand-written list beside a growing set goes stale on the first Kafka release.
        val known = ApacheProducerConfig.configNames()
        val unknown = config.properties.keys.filterNot { it in known }.sorted()
        require(unknown.isEmpty()) {
            "unknown producer configuration: ${unknown.joinToString()} " +
                "(kafka-clients knows ${known.size} keys; nothing here is silently ignored)"
        }
    }

    private val delegate = org.apache.kafka.clients.producer.KafkaProducer<ByteArray, ByteArray>(
        Properties().apply {
            config.properties.forEach { (key, value) -> setProperty(key, value) }
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
        suspendCancellableCoroutine { continuation ->
            delegate.send(ApacheRecord(record.topic, record.key, record.value)) { metadata, failure ->
                when {
                    failure != null -> continuation.resumeWithException(failure)
                    else -> continuation.resume(
                        RecordMetadata(metadata.topic(), metadata.partition(), metadata.offset()),
                    )
                }
            }
        }

    override suspend fun flush() {
        delegate.flush()
    }

    override suspend fun close() {
        delegate.close()
    }
}
