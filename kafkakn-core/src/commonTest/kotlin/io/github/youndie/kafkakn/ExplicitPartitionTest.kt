package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.time.TimeSource

/**
 * [B-27](../../../../../../../docs/backlog/B-27-a-record-can-name-its-partition.md): a record can name
 * its partition, as it can in the Java client (`ProducerRecord(topic, partition, …)`) and in
 * librdkafka (`RD_KAFKA_VTYPE_PARTITION` of `rd_kafka_produceva`).
 *
 * **Half of the first assertion is in `ci/b-27/run.sh`**, as it is for [AccountingTest]. The partition
 * in [RecordMetadata] is the client's report of where it sent the record; the broker's own consumer,
 * reading one partition at a time, is the party that says where the record is.
 */
class ExplicitPartitionTest {
    private fun producer() = kafkaProducer(ProducerConfig("bootstrap.servers" to bootstrap))

    private fun bytes(text: String) = text.encodeToByteArray()

    @Test
    fun records_land_on_the_partition_they_name() =
        runTest {
            withContext(Dispatchers.Default) {
                val stamp = "part-$armName-${randomSuffix()}"
                val producer = producer()
                try {
                    for (partition in 0 until PARTITIONS) {
                        repeat(PER_PARTITION) { index ->
                            val value = "$stamp:$partition:$index".encodeToByteArray()
                            val where = producer.send(ProducerRecord(testTopic, value, partition = partition))
                            assertEquals(partition, where.partition, "the record was sent somewhere it did not name")
                        }
                    }
                } finally {
                    producer.close()
                }
                recordArmFact("partition.stamp", stamp)
                recordArmFact("partition.per", PER_PARTITION.toString())
            }
        }

    @Test
    fun a_named_partition_overrides_what_the_key_hashes_to() =
        runTest {
            withContext(Dispatchers.Default) {
                val producer = producer()
                try {
                    val key = "partition-override-${randomSuffix()}".encodeToByteArray()
                    // Where the partitioner puts this key, asked of the arm itself rather than computed
                    // here - the test is about the override, not about murmur2.
                    val unnamed = ProducerRecord(testTopic, bytes("hashed"), key = key)
                    val hashed = producer.send(unnamed).partition
                    val named = (hashed + 1) % PARTITIONS
                    val where = producer.send(ProducerRecord(testTopic, bytes("named"), key = key, partition = named))
                    assertEquals(named, where.partition, "the key won over the partition the caller named")
                } finally {
                    producer.close()
                }
            }
        }

    @Test
    fun a_negative_partition_is_refused_where_the_record_is_made() {
        assertFails { ProducerRecord(testTopic, ByteArray(0), partition = -1) }
    }

    @Test
    fun a_partition_the_topic_does_not_have_fails_and_each_arm_says_so_its_own_way() =
        runTest {
            withContext(Dispatchers.Default) {
                val config = ProducerConfig(mapOf("bootstrap.servers" to bootstrap) + failFastConfig())
                val producer = kafkaProducer(config)
                val started = TimeSource.Monotonic.markNow()
                val failure =
                    try {
                        val nowhere = ProducerRecord(testTopic, bytes("nowhere"), partition = MISSING)
                        assertFails { producer.send(nowhere) }
                    } finally {
                        producer.close()
                    }
                // Recorded rather than compared: the two clients take different routes to the same
                // refusal, and how long each takes is part of what a caller meets.
                val said = failure.message.orEmpty().replace('\n', ' ')
                recordArmFact("partition.missing.failure", said.take(REASON))
                val waited = started.elapsedNow().inWholeMilliseconds
                recordArmFact("partition.missing.ms", waited.toString())
            }
        }

    private companion object {
        /** What `ci/harness/broker.sh topic` creates. */
        const val PARTITIONS = 3
        const val PER_PARTITION = 50
        const val MISSING = 99
        const val REASON = 300
    }
}
