package io.github.youndie.kafkakn

/**
 * One record on its way to a topic.
 *
 * Key and value are **bytes**, not `String`. Kafka's key and value space has no encoding; a
 * `String` API imposes UTF-8 on it, and the first payload that is not valid UTF-8 is the one that
 * finds out. Callers that want text encode it themselves, where they can see the decision.
 *
 * Deliberately not a `data class`: the generated `equals` would compare the two `ByteArray`s by
 * identity, so two records with the same bytes would be unequal and nobody would notice until a
 * test compared them.
 */
public class ProducerRecord(
    public val topic: String,
    public val value: ByteArray,
    public val key: ByteArray? = null,
    public val headers: List<RecordHeader> = emptyList(),
    /**
     * The partition this record goes to, or `null` to let the partitioner choose — which is the
     * default and what every record before this field did.
     *
     * When it is set the partitioner is **not consulted**, key or no key: a caller who names a
     * partition is managing placement themselves, and a partitioner second-guessing them would put a
     * keyed record somewhere its caller did not ask for. A partition the topic does not have fails at
     * `send`, on both arms, and how each arm says so is in the contract rather than promised equal.
     */
    public val partition: Int? = null,
    /**
     * When this record happened, in milliseconds since the epoch, or `null` for "now" — the client's
     * clock at the moment the record is handed to it, which is what every record before this field
     * carried.
     *
     * Kafka's own unit, not `kotlin.time.Instant`: the wire carries a `Long` of milliseconds, and a
     * caller holding an instant converts it where they can see the decision. On a topic configured
     * with `message.timestamp.type=LogAppendTime` the broker replaces it with its own clock, and
     * [RecordMetadata.timestamp] is how a caller learns which one was kept.
     */
    public val timestamp: Long? = null,
) {
    init {
        // Here, where the record is made, rather than at `send`: both clients refuse a negative
        // partition, but at different moments and in different words, and this one is not a question
        // about the broker at all. librdkafka's own "unassigned" is -1, which is exactly the value a
        // caller could pass by mistake and have silently mean "let the partitioner choose".
        require(partition == null || partition >= 0) { "partition must not be negative, was $partition" }
        // Both clients refuse a negative timestamp, at different moments; and -1 is what both use
        // internally for "no timestamp", which a caller could pass meaning a date.
        require(timestamp == null || timestamp >= 0) { "timestamp must not be negative, was $timestamp" }
    }

    override fun toString(): String =
        "ProducerRecord(topic=$topic, key=${key?.size ?: 0} bytes, value=${value.size} bytes, " +
            "headers=${headers.size}, partition=${partition ?: "any"}, timestamp=${timestamp ?: "now"})"
}

/**
 * One header on a record: a name, and bytes that mean whatever the reader agrees they mean.
 *
 * **A list, not a map**, and that is Kafka's shape rather than a simplification of it. The protocol
 * carries an ordered sequence in which **a name may appear more than once**, and a consumer reading
 * `headers.lastHeader(name)` gets a different answer from one iterating them. Flattening that into a
 * `Map<String, ByteArray>` would silently drop entries — for tracing baggage and schema identifiers,
 * exactly the entries somebody put there on purpose.
 *
 * [value] is nullable because the protocol says so: a header may carry a null value, which is not
 * the same as an empty one, and a consumer can tell them apart.
 */
public class RecordHeader(
    public val name: String,
    public val value: ByteArray?,
) {
    override fun toString(): String = "RecordHeader($name=${value?.size ?: "null"} bytes)"
}

/**
 * Where a record landed, as the broker reported it.
 *
 * Returned only after the broker has acknowledged at the configured `acks`, so its existence is the
 * acknowledgement — there is no state in which a caller holds one of these and the record was not
 * accepted.
 */
public data class RecordMetadata(
    public val topic: String,
    public val partition: Int,
    public val offset: Long,
    /**
     * The record's timestamp **as the broker kept it**, in milliseconds since the epoch: the one the
     * record carried on an ordinary topic, and the broker's own clock on a topic configured with
     * `message.timestamp.type=LogAppendTime` — which is the only way a caller can learn that the
     * time they set was replaced.
     *
     * There is no field saying which of the two it was, and that is a finding rather than an
     * omission: the Java client's `RecordMetadata` exposes the value and not its type, so a type here
     * would be a field one arm could fill and the other could only guess.
     */
    public val timestamp: Long,
)
