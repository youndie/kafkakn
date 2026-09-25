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

    /**
     * The partitions of [topic], as this producer's connection to the cluster sees them, ordered by
     * partition id ([B-29](../../../../../../../docs/backlog/B-29-topic-metadata.md)).
     *
     * **Suspends, and waits off the caller's dispatcher on both arms.** Both clients answer this with
     * a blocking call — `Producer.partitionsFor` for up to `max.block.ms`, `rd_kafka_metadata` for up
     * to its timeout — and a suspend signature over a blocking call holds the thread it was called on.
     *
     * An unknown topic fails. How, and after how long, differs per arm and is recorded in the
     * contract rather than promised here.
     */
    public suspend fun partitionsFor(topic: String): List<PartitionInfo>

    /**
     * Registers this producer's `transactional.id` with the cluster and fences any older producer
     * holding the same one ([B-30](../../../../../../../docs/backlog/B-30-transactions.md)). Once,
     * before the first [beginTransaction]. Blocks inside both clients, so it waits off the caller's
     * dispatcher.
     *
     * Transactions are Kafka's, with Kafka's names: records sent between [beginTransaction] and
     * [commitTransaction] become visible to a `read_committed` reader together, and after
     * [abortTransaction] none of them do. A producer another one has fenced throws
     * [ProducerFencedException] from its next transactional call, on both arms, and is finished.
     */
    public suspend fun initTransactions()

    /** Starts a transaction. Every [send] until [commitTransaction] or [abortTransaction] belongs to it. */
    public suspend fun beginTransaction()

    /**
     * Commits a consumer's progress **inside** the open transaction, so that the records this producer
     * sent in it and the input positions they came from become visible together or not at all —
     * exactly-once read-process-write ([B-38](../../../../../../../docs/backlog/B-38-exactly-once-read-process-write.md)).
     *
     * [offsets] are the next offset to read per partition — one past the last record processed — and
     * [group] is [KafkaConsumer.groupMetadata], taken from the consumer that read them. The loop around
     * it is the caller's.
     */
    public suspend fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, Long>,
        group: ConsumerGroupMetadata,
    )

    /** Flushes, then commits: every record of the transaction becomes visible to `read_committed` readers. */
    public suspend fun commitTransaction()

    /** Aborts: none of the transaction's records become visible to `read_committed` readers. */
    public suspend fun abortTransaction()

    /** The producer's machinery now: buffered bytes, requests in flight, broker round trip, connections ([ProducerMetrics]). */
    public suspend fun metrics(): ProducerMetrics

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
 * One partition of a topic, as the cluster described it.
 *
 * Node ids rather than nodes: both clients describe a broker by id, host and port, and the id is the
 * part a caller can compare with `kafka-topics.sh --describe`. [leader] is null when the partition
 * has none — the Java client reports that as a null or `Node.noNode()`, librdkafka as `-1`, and one
 * answer is what a caller should have to handle.
 */
public data class PartitionInfo(
    public val topic: String,
    public val partition: Int,
    public val leader: Int?,
    public val replicas: List<Int>,
    public val inSyncReplicas: List<Int>,
)
