package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * [B-47](../../../../../../../docs/backlog/B-47-a-producer-can-write-a-tombstone.md): a record whose
 * value is null is a tombstone, and a record whose value is empty is not.
 *
 * Kafka's protocol carries a null value, and a compacted topic reads it as "this key is deleted". An
 * empty value is a value. Two keys per arm, on the compacted fixture:
 * - `<stamp>-gone`: a value, then a null value. After compaction, the value is gone and the key's only
 *   record is the tombstone.
 * - `<stamp>-kept`: a value, then an empty value. After compaction, the empty value is what remains.
 *
 * **What the broker stored is not something either arm can be asked**, so the test records the stamp,
 * and `ci/b-47/run.sh` reads the topic back with the distribution's own client, which prints a null as
 * `~` and bytes as `x<hex>`. It checks each key's last record, and then what compaction leaves.
 */
class TombstoneTest {
    @Test
    fun a_null_value_is_a_tombstone_and_an_empty_value_is_not() =
        runTest {
            withContext(Dispatchers.Default) {
                val stamp = "tomb-$armName-${randomSuffix()}"
                val producer = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"))
                try {
                    for ((name, last) in listOf("$stamp-gone" to null, "$stamp-kept" to ByteArray(0))) {
                        val key = name.encodeToByteArray()
                        producer.send(ProducerRecord(compactTopic, BEFORE.encodeToByteArray(), key = key))
                        val landed = producer.send(ProducerRecord(compactTopic, last, key = key))
                        assertEquals(0, landed.partition, "the compacted fixture has one partition")
                    }
                } finally {
                    producer.close()
                }
                recordArmFact("tombstone.stamp", stamp)
                // What both arms must agree on: that the two are different records. Whether each arm put
                // them on the wire as different records is the broker's to say, in ci/b-47/run.sh.
                recordObservation("tombstone.null.is.not.empty", "true")
            }
        }

    private companion object {
        const val BEFORE = "before"
    }
}
