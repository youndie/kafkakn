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
) {
    override fun toString(): String =
        "ProducerRecord(topic=$topic, key=${key?.size ?: 0} bytes, value=${value.size} bytes)"
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
