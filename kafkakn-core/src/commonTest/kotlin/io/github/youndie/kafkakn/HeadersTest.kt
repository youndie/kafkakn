package io.github.youndie.kafkakn

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Headers, on the path that is not variadic.
 *
 * [feature-produce-a-record](../../../../../../../docs/features/feature-produce-a-record.md).
 * Headers are how tracing context and schema identifiers travel, so a producer without them is not
 * usable in most deployments — and the obvious vehicle, `rd_kafka_producev`, is variadic and cannot
 * be called through cinterop at all (research §1.5).
 *
 * **What the broker stored is not something either arm can be asked.** The renderings recorded here
 * are what each arm *intended*; `ci/b-10/run.sh` reads what actually landed with
 * `kafka-console-consumer --property print.headers=true` and holds it against both — and against
 * the other arm, which is where the two clients could disagree while each looks right alone.
 */
class HeadersTest {
    @Test
    fun headers_reach_the_broker_with_their_bytes_intact() =
        runTest {
            val stamp = "hdr-$armName-${randomSuffix()}"
            // Duplicate name, on purpose and in this order. Kafka's headers are an ordered sequence in
            // which a name may repeat; a client that stored them in a map would answer this test with
            // two entries instead of three, and a consumer reading `lastHeader` would get "2" where an
            // iterating one gets "1".
            val headers =
                listOf(
                    RecordHeader("trace", "1".encodeToByteArray()),
                    RecordHeader("schema", "kafkakn.v1".encodeToByteArray()),
                    RecordHeader("trace", "2".encodeToByteArray()),
                )
            send(stamp, headers)

            recordArmFact("headers.stamp", stamp)
            recordArmFact("headers.expected", headers.joinToString(",") { "${it.name}:${it.value?.decodeToString()}" })
            // The same claim in the file the arms are diffed against: what a client puts on the wire for
            // one record is something only the client knows until somebody reads it back.
            recordObservation("headers.order", headers.joinToString(",") { it.name })
            recordObservation("headers.count", headers.size.toString())
        }

    @Test
    fun a_header_with_no_value_is_not_a_header_with_an_empty_one() =
        runTest {
            val stamp = "hdrnull-$armName-${randomSuffix()}"
            send(
                stamp,
                listOf(
                    RecordHeader("absent", null),
                    RecordHeader("empty", ByteArray(0)),
                ),
            )
            recordArmFact("headers.null.stamp", stamp)
            // Kafka's protocol carries a null header value, and it is not the same as a zero-length one.
            // Both arms must make the same distinction, or the contract is hiding a difference rather
            // than covering one.
            recordObservation("headers.null.distinct", "absent!=empty")
        }

    @Test
    fun a_record_without_headers_still_produces() =
        runTest {
            // The path changed for everybody, not only for callers who want headers: both cases now go
            // through rd_kafka_produceva on the native arm. A record with no headers is the case that
            // was working before and is the one a regression would hit first.
            val stamp = "hdrnone-$armName-${randomSuffix()}"
            val metadata = send(stamp, emptyList())
            assertEquals(testTopic, metadata.topic)
            recordArmFact("headers.none.stamp", stamp)
        }

    private suspend fun send(
        stamp: String,
        headers: List<RecordHeader>,
    ): RecordMetadata {
        val producer =
            kafkaProducer(
                ProducerConfig("bootstrap.servers" to bootstrap, "acks" to "all"),
            )
        try {
            return withContext(Dispatchers.Default) {
                val metadata =
                    producer.send(
                        ProducerRecord(testTopic, stamp.encodeToByteArray(), headers = headers),
                    )
                producer.flush()
                metadata
            }
        } finally {
            producer.close()
        }
    }
}
