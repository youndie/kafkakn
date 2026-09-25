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
     * [subscribe], and tells [listener] when partitions arrive and leave (consumer-contract §2a,
     * [B-50](../../../../../../../docs/backlog/B-50-a-rebalance-listener.md)). The callbacks run inside
     * [poll] (and inside [close], which revokes), on the thread that is polling. They are plain
     * functions, and they must not call this consumer: that would wait for the `poll` that is waiting for
     * them, so on both arms it throws instead. [RebalanceScope] is the one way back into the client from
     * inside a callback.
     */
    public suspend fun subscribe(
        topics: List<String>,
        listener: RebalanceListener,
    )

    /**
     * Commits, synchronously, the position after every record [poll] has returned — for every partition
     * this consumer holds. Needs a `group.id` the caller named.
     */
    public suspend fun commit()

    /**
     * Commits, synchronously, exactly these offsets
     * ([B-48](../../../../../../../docs/backlog/B-48-commit-explicit-offsets.md)). Each value is the **next
     * offset to read** for its partition, the position after the last record processed, as
     * [KafkaProducer.sendOffsetsToTransaction] takes it. This is how a caller who processes records one
     * at a time commits what was processed rather than everything [poll] returned. Needs a `group.id`
     * the caller named. An empty map commits nothing; a negative offset is refused here, on both arms.
     */
    public suspend fun commit(offsets: Map<TopicPartition, Long>)

    /**
     * What a transactional producer needs to commit this consumer's progress inside its transaction —
     * [KafkaProducer.sendOffsetsToTransaction] ([B-38](../../../../../../../docs/backlog/B-38-exactly-once-read-process-write.md)).
     * Opaque, taken fresh for each transaction, and good only for a producer on the same arm in the
     * same process. Needs a `group.id` the caller named.
     */
    public suspend fun groupMetadata(): ConsumerGroupMetadata

    /**
     * The offset of the next record [poll] will return for [partition]
     * ([B-49](../../../../../../../docs/backlog/B-49-committed-and-position.md)). Before any record has been
     * read it is still a number, the same on both arms: where the last seek put it, or else what the
     * group committed, or else where `auto.offset.reset` says. Answering may ask the broker. A partition
     * this consumer is not assigned is refused with [IllegalStateException].
     */
    public suspend fun position(partition: TopicPartition): Long

    /**
     * What this consumer's group has committed for each of [partitions]: the next offset to read, or
     * null where nothing is committed. Asks the group's coordinator. Needs a `group.id` the caller named.
     */
    public suspend fun committed(partitions: List<TopicPartition>): Map<TopicPartition, Long?>

    /**
     * Stops returning records from [partitions] without leaving the group
     * ([B-52](../../../../../../../docs/backlog/B-52-pause-and-resume.md)): keep calling [poll], which
     * returns the other partitions' records and keeps this member alive past `max.poll.interval.ms`. The
     * consumer's backpressure, as a suspending `send` is the producer's. A partition not held is refused
     * with [IllegalStateException]. A rebalance that takes a partition away also forgets it was paused.
     */
    public suspend fun pause(partitions: List<TopicPartition>)

    /** Returns records from [partitions] again, from where they stopped. */
    public suspend fun resume(partitions: List<TopicPartition>)

    /** The partitions paused now, in topic-then-partition order. */
    public suspend fun paused(): List<TopicPartition>

    /** What this consumer's client reports about itself ([B-53](../../../../../../../docs/backlog/B-53-consumer-lag-in-metrics.md)). */
    public suspend fun metrics(): ConsumerMetrics

    /** The partitions this consumer holds now: its [assign]ment, or its share of a group. */
    public suspend fun assignment(): List<TopicPartition>

    /**
     * Moves where [partition] is read from next. The partition must be held: [assign]ed, or given to this
     * member by its group ([B-51](../../../../../../../docs/backlog/B-51-seek-under-a-subscription.md)). A
     * partition not held is refused with [IllegalStateException], on both arms. To seek a partition as a
     * group hands it over, use [RebalanceScope.seek] from the listener.
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

/**
 * Told when this member's partitions change (consumer-contract §2a). Every callback runs inside `poll` or
 * `close`, before that call returns, on the thread that made it. None is called with an empty list, on
 * either arm: the Java client calls `onPartitionsAssigned` with nothing on some rebalances, and librdkafka
 * does not.
 */
public interface RebalanceListener {
    /**
     * These partitions are about to leave this member. Commit what was processed on them, through
     * [scope], here: once this returns they may belong to another member.
     */
    public fun onRevoked(
        partitions: List<TopicPartition>,
        scope: RebalanceScope,
    ) {}

    /** These partitions arrived. Called before any of their records is returned by `poll`. */
    public fun onAssigned(partitions: List<TopicPartition>) {}

    /**
     * The same moment, with a [scope] to [seek][RebalanceScope.seek] the partitions that arrived to where
     * reading should start ([B-51](../../../../../../../docs/backlog/B-51-seek-under-a-subscription.md)).
     * This is the one both arms call. By default it calls the one-argument form, so a listener overrides
     * whichever it needs.
     */
    public fun onAssigned(
        partitions: List<TopicPartition>,
        scope: RebalanceScope,
    ) {
        onAssigned(partitions)
    }

    /**
     * These partitions were taken without a revocation: the member was removed from the group, for
     * example after missing `session.timeout.ms`. A commit would be refused, so there is no scope.
     */
    public fun onLost(partitions: List<TopicPartition>) {}
}

/** What a rebalance callback may do with the client that is running it. */
public interface RebalanceScope {
    /**
     * Moves where [partition] is read from, from inside `onAssigned` (B-51), taking effect before the
     * first of its records is returned. Only for a partition that just arrived. From `onRevoked` it is
     * refused: the partition is leaving.
     */
    public fun seek(
        partition: TopicPartition,
        to: SeekTo,
    )

    /**
     * Commits these offsets synchronously, inside the callback. The same meaning as
     * [KafkaConsumer.commit] with offsets: each value is the next offset to read.
     */
    public fun commit(offsets: Map<TopicPartition, Long>)
}

/** Thrown when a callback calls the consumer that is running it (consumer-contract §2a). */
internal fun refuseReentry(call: String): Nothing =
    throw IllegalStateException(
        "$call was called from inside a rebalance callback, which runs inside poll or close: it would wait " +
            "for the call that is waiting for it. Use the RebalanceScope the callback was given.",
    )

/**
 * The consumer's machinery, as [ProducerMetrics] is the producer's: nothing here counts records handled.
 *
 * [lag] is, per held partition, the end of the log minus this consumer's position: how many records it
 * has not read yet. The end is the last stable offset under `read_committed`, which is the contract's
 * default. Null where the client does not know it yet, before its first fetch of the partition. The two
 * arms measure at different moments: the Java client on each fetch, the native arm once per statistics
 * interval (a second, delivered inside `poll`). So two readings taken while records are still arriving
 * can differ by what arrived in between, and a lag that is not moving reads the same on both.
 */
public class ConsumerMetrics(
    public val lag: Map<TopicPartition, Long?>,
) {
    override fun toString(): String = "ConsumerMetrics(lag=$lag)"
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

/** An offset to commit is the next one to read: never negative. Checked in common code, once for both arms. */
internal fun requireCommittable(offsets: Map<TopicPartition, Long>) {
    offsets.forEach { (partition, offset) ->
        require(offset >= 0) { "an offset to commit must not be negative: $partition -> $offset" }
    }
}

/**
 * The keys the KIP-848 group protocol moves to the broker
 * ([B-57](../../../../../../../docs/backlog/B-57-the-kip-848-consumer-protocol.md)). Under
 * `group.protocol=consumer` the broker assigns and times the group, and both clients refuse these three at
 * construction, in different types: `ConfigException` from the Java client, a failed `rd_kafka_new` from
 * librdkafka. Refused here first, in one type on both arms. The assignor is named with `group.remote.assignor`
 * instead, and the timeouts are the broker's `group.consumer.*` settings.
 */
internal fun ConsumerConfig.checkGroupProtocolKeys() {
    if (properties["group.protocol"]?.trim()?.lowercase() != "consumer") return
    val classicOnly = CLASSIC_PROTOCOL_KEYS.filter { it in properties }
    require(classicOnly.isEmpty()) {
        "${classicOnly.joinToString()}: not honoured under group.protocol=consumer, where the broker assigns and " +
            "times the group; name the assignor with group.remote.assignor, and the timeouts are the broker's"
    }
}

private val CLASSIC_PROTOCOL_KEYS =
    listOf("partition.assignment.strategy", "session.timeout.ms", "heartbeat.interval.ms")

/**
 * `partition.assignment.strategy`, in the one spelling both arms honour
 * ([B-55](../../../../../../../docs/backlog/B-55-cooperative-rebalancing.md)): librdkafka's words, which the
 * JVM arm translates into the Java client's class names. A value only one arm understands (a Java class
 * name, `sticky`) is refused at construction, and so is cooperative next to an eager assignor: librdkafka
 * runs one protocol at a time. Unset, each arm keeps its client's own default.
 */
internal fun ConsumerConfig.assignmentStrategy(): List<String>? {
    val words = properties["partition.assignment.strategy"]?.split(",")?.map { it.trim() } ?: return null
    words.forEach { word ->
        require(word in PORTABLE_ASSIGNORS) {
            "partition.assignment.strategy: '$word' is not honoured by both arms; the portable values are " +
                PORTABLE_ASSIGNORS.joinToString()
        }
    }
    require(COOPERATIVE !in words || words.size == 1) {
        "partition.assignment.strategy: $COOPERATIVE cannot be combined with an eager assignor ($words)"
    }
    return words
}

/**
 * The portable assignor words. The Java client's class for each is the JVM arm's business, not common
 * code's (`scripts/common_is_platform_free.py` refuses a Java package here).
 */
internal val PORTABLE_ASSIGNORS: Set<String> = setOf("range", "roundrobin", COOPERATIVE)

private const val COOPERATIVE = "cooperative-sticky"

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

/**
 * Another member joined the group with this member's `group.instance.id`, and the broker fenced this one
 * ([B-56](../../../../../../../docs/backlog/B-56-static-membership.md)): its `poll` throws this, and the only
 * thing left to do with it is [KafkaConsumer.close].
 *
 * **One exception for both arms**, as [ProducerFencedException] is for producers. The Java client throws its
 * own `FencedInstanceIdException`, kept as the [cause]; librdkafka reports a fatal error whose reason is
 * `FENCED_INSTANCE_ID`, and its sentence stays in the message.
 */
public class ConsumerFencedException(
    message: String,
    cause: Throwable? = null,
) : IllegalStateException(message, cause)

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
