package io.github.youndie.kafkakn

/**
 * A Kafka producer.
 *
 * The contract is [docs/api/producer-contract.md]; the promise that shapes everything else is that
 * **every call to [send] that returns normally corresponds to one record the broker acknowledged,
 * and every call that does not return normally throws.** There is no third outcome, and in
 * particular no outcome in which the caller must inspect a count to learn whether their record
 * survived.
 *
 * That is not how the underlying native library behaves — `rd_kafka_produce` enqueues, refuses when
 * its queue is full, and a record it never queued produces no delivery report at all — which is
 * exactly why the contract is written down and the implementation is held to it.
 */
public interface KafkaProducer {

    /**
     * Sends [record] and returns where it landed, after the broker has acknowledged it.
     *
     * **Suspends** while the record cannot yet be accepted — a full queue is backpressure, not an
     * error. Throws only for failures the producer cannot retry: an unknown topic where
     * auto-creation is off, an invalid configuration, a producer already closed.
     */
    public suspend fun send(record: ProducerRecord): RecordMetadata

    /** Returns when every record handed to [send] has been acknowledged or has failed. */
    public suspend fun flush()

    /**
     * Flushes, then releases.
     *
     * **Suspending, and therefore not [AutoCloseable].** Kotlin's `AutoCloseable.close` cannot
     * suspend, and the two ways to fit into it are both wrong here: blocking a thread inside
     * `close` on a runtime built around coroutines, or dropping records that are still in flight.
     * A record accepted by [send] before [close] is either acknowledged or its `send` throws.
     */
    public suspend fun close()
}

/** Creates a producer. The implementation is the platform's: librdkafka on native, the official client on the JVM. */
public expect fun kafkaProducer(config: ProducerConfig): KafkaProducer

/**
 * The stub both arms return until they are implemented.
 *
 * Shared between the two actuals on purpose: while neither exists, replacing one arm's factory is
 * then a visible, isolated change rather than an edit that leaves the other arm looking the same
 * but meaning something different.
 */
internal object UnimplementedProducer : KafkaProducer {
    override suspend fun send(record: ProducerRecord): RecordMetadata = TODO("no producer yet")
    override suspend fun flush(): Unit = TODO("no producer yet")
    override suspend fun close(): Unit = TODO("no producer yet")
}
