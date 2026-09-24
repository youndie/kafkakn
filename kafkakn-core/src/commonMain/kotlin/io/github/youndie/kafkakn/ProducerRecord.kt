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
) {
    init {
        // Here, where the record is made, rather than at `send`: both clients refuse a negative
        // partition, but at different moments and in different words, and this one is not a question
        // about the broker at all. librdkafka's own "unassigned" is -1, which is exactly the value a
        // caller could pass by mistake and have silently mean "let the partitioner choose".
        require(partition == null || partition >= 0) { "partition must not be negative, was $partition" }
    }

    override fun toString(): String =
        "ProducerRecord(topic=$topic, key=${key?.size ?: 0} bytes, value=${value.size} bytes, " +
            "headers=${headers.size}, partition=${partition ?: "any"})"
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
)
