package io.github.youndie.kafkakn

import kotlin.time.Duration

/**
 * A Kafka consumer, without group coordination yet: partitions are assigned explicitly and read
 * ([B-36](../../../../../../../docs/backlog/B-36-assign-and-poll.md)).
 *
 * The contract is [docs/api/consumer-contract.md], designed before this was written (B-35). Two
 * decisions from it shape every call here:
 *
 * - **Calls never overlap on one consumer.** The Java client throws on overlapping calls, so the JVM
 *   arm runs every call on a serial lane of its own; librdkafka does not care, and the native arm
 *   polls without waiting inside C. A caller may call from several coroutines at once and the calls
 *   are queued, never interleaved — which is not parallelism: a caller who wants that runs more
 *   consumers.
 * - **`poll` is explicit.** The time between two `poll`s is Kafka's `max.poll.interval.ms` on both
 *   arms, and a caller who can see their own loop can see that time.
 */
public interface KafkaConsumer {
    /** Reads exactly these partitions from now on, replacing any earlier assignment. */
    public suspend fun assign(partitions: List<TopicPartition>)

    /**
     * Joins the consumer's group and reads [topics], with the partitions shared among the group's
     * members and handed over when one leaves ([B-37](../../../../../../../docs/backlog/B-37-consumer-groups.md)).
     * Needs a `group.id` the caller named. A consumer does one or the other: [assign] or this.
     *
     * **At-least-once, and only with [commit].** Auto-commit is off on both arms (consumer-contract
     * §3), so a partition handed to another member resumes from the last offset committed for it:
     * records returned but not committed are delivered again, and none is skipped.
     */
    public suspend fun subscribe(topics: List<String>)

    /**
     * Commits, synchronously, the position after every record [poll] has returned — for every partition
     * this consumer holds. Needs a `group.id` the caller named.
     */
    public suspend fun commit()

    /**
     * What a transactional producer needs to commit this consumer's progress inside its transaction —
     * [KafkaProducer.sendOffsetsToTransaction] ([B-38](../../../../../../../docs/backlog/B-38-exactly-once-read-process-write.md)).
     * Opaque, taken fresh for each transaction, and good only for a producer on the same arm in the
     * same process. Needs a `group.id` the caller named.
     */
    public suspend fun groupMetadata(): ConsumerGroupMetadata

    /** The partitions this consumer holds now: its [assign]ment, or its share of a group. */
    public suspend fun assignment(): List<TopicPartition>

    /**
     * Moves where [partition] is read from next. The partition must be [assign]ed; under a
     * subscription the group decides positions, and a seek is refused on both arms for now.
     */
    public suspend fun seek(
        partition: TopicPartition,
        to: SeekTo,
    )

    /**
     * The records available now, up to a bound — 500, the Java client's `max.poll.records` default
     * and the native arm's per-call limit — or an empty list once [timeout] passes with none.
     *
     * Suspends while it waits, without holding the caller's dispatcher, and returns promptly when
     * cancelled; the consumer stays usable after a cancelled `poll`.
     */
    public suspend fun poll(timeout: Duration): List<ConsumerRecord>

    /** Releases the consumer. */
    public suspend fun close()
}

/** Creates a consumer. The implementation is the platform's, as for [kafkaProducer]. */
public expect fun kafkaConsumer(config: ConsumerConfig): KafkaConsumer

/**
 * The consumer's configuration, in Kafka's own keys, held to the producer's rules: a key neither
 * client honours fails at construction, and TLS and SASL are spelled and translated as for a producer.
 *
 * Four defaults differ from the clients' own, on purpose and on both arms (consumer-contract §3):
 * `isolation.level=read_committed`, `enable.auto.commit=false`, `allow.auto.create.topics=false`,
 * `check.crcs=true`. A caller who sets any of them keeps their value.
 */
public class ConsumerConfig(
    public val properties: Map<String, String>,
) {
    public constructor(vararg pairs: Pair<String, String>) : this(pairs.toMap())

    override fun toString(): String = "ConsumerConfig(${properties.keys.sorted()})"
}

/**
 * Whether the caller named a group. `subscribe` and `commit` need one on both arms; the native arm's
 * private `kafkakn-assign-*` id (B-36) exists only so librdkafka will `assign`, and must never be the
 * group a subscription joins or a commit lands in.
 */
internal fun ConsumerConfig.namesAGroup(): Boolean = !properties["group.id"].isNullOrEmpty()

internal fun requireGroup(
    named: Boolean,
    call: String,
) {
    check(named) { "$call needs a group.id: set one in ConsumerConfig. assign() and poll() work without it" }
}

/** The defaults consumer-contract §3 sets on both arms, under whatever the caller wrote. */
internal fun ConsumerConfig.withContractDefaults(): Map<String, String> = CONTRACT_DEFAULTS + properties

private val CONTRACT_DEFAULTS =
    mapOf(
        // librdkafka's default, and the safe one: read_uncommitted shows records from aborted
        // transactions, which as far as their producer's caller is concerned never happened.
        "isolation.level" to "read_committed",
        // One value that commits different things on the two clients; off until the caller commits.
        "enable.auto.commit" to "false",
        // Reading a topic does not create it.
        "allow.auto.create.topics" to "false",
        // Corruption surfaces as an error rather than as bytes.
        "check.crcs" to "true",
    )

/** Topic, then partition: the order [KafkaConsumer.assignment] returns on both arms. */
internal val PARTITION_ORDER: Comparator<TopicPartition> = compareBy({ it.topic }, { it.partition })

/** A partition of a topic. */
public data class TopicPartition(
    public val topic: String,
    public val partition: Int,
) {
    init {
        require(partition >= 0) { "partition must not be negative, was $partition" }
    }
}

/** Where [KafkaConsumer.seek] moves a partition to. */
public sealed interface SeekTo {
    /** The earliest offset the broker still has. */
    public data object Beginning : SeekTo

    /** The offset after the last record: only what is written from now on is read. */
    public data object End : SeekTo

    /** An absolute offset. */
    public data class Offset(
        public val offset: Long,
    ) : SeekTo {
        init {
            require(offset >= 0) { "offset must not be negative, was $offset" }
        }
    }

    /**
     * The earliest offset whose timestamp is at or after [timestamp], as the broker's time index says
     * — and the end of the partition if there is none.
     */
    public data class Timestamp(
        public val timestamp: Long,
    ) : SeekTo {
        init {
            require(timestamp >= 0) { "timestamp must not be negative, was $timestamp" }
        }
    }
}

/**
 * A record as the broker stored it. Bytes, never strings, and [key] and [value] are each null when
 * the record carried none — a tombstone is a null value, and a consumer reads whatever anyone wrote.
 * [headers] keep their order and their duplicates.
 */
public class ConsumerRecord(
    public val topic: String,
    public val partition: Int,
    public val offset: Long,
    public val timestamp: Long,
    public val key: ByteArray?,
    public val value: ByteArray?,
    public val headers: List<RecordHeader>,
) {
    override fun toString(): String =
        "ConsumerRecord($topic-$partition@$offset, key=${key?.size ?: "null"} bytes, " +
            "value=${value?.size ?: "null"} bytes, headers=${headers.size})"
}

/**
 * A consumer's group membership as its client describes it: group id, generation, member id. Opaque on
 * purpose — each arm carries its own client's object, and nothing here reads it apart from handing it
 * to [KafkaProducer.sendOffsetsToTransaction].
 */
public abstract class ConsumerGroupMetadata internal constructor() {
    /** The group the progress belongs to. */
    public abstract val groupId: String
}
