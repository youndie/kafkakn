package io.github.youndie.kafkakn

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerConfig.configNames
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.Properties
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import org.apache.kafka.clients.consumer.ConsumerRecord as ApacheRecord
import org.apache.kafka.clients.consumer.KafkaConsumer as ApacheConsumer
import org.apache.kafka.common.TopicPartition as ApachePartition

/** The JVM arm of the consumer: the Java client, delegated to, on a lane of its own. */
public actual fun kafkaConsumer(config: ConsumerConfig): KafkaConsumer = JvmKafkaConsumer(config)

internal class JvmKafkaConsumer(
    config: ConsumerConfig,
) : KafkaConsumer {
    init {
        ProducerConfig(config.properties).run {
            checkTlsKeys()
            checkSaslKeys()
        }
    }

    private val properties = translateForJava(config.withContractDefaults())

    init {
        val known = configNames()
        val unknown = properties.keys.filterNot { it in known }.sorted()
        require(unknown.isEmpty()) {
            "unknown consumer configuration: ${unknown.joinToString()} " +
                "(kafka-clients knows ${known.size} consumer keys; nothing here is silently ignored)"
        }
    }

    private val delegate: ApacheConsumer<ByteArray?, ByteArray?> =
        ApacheConsumer(
            Properties().apply { properties.forEach { (key, value) -> setProperty(key, value) } },
            ByteArrayDeserializer(),
            ByteArrayDeserializer(),
        )

    /**
     * The one place every call to [delegate] runs (consumer-contract §1).
     *
     * The Java consumer's "not thread-safe" is a lock held for ONE call — `acquire()` takes the calling
     * thread's id and `release()` clears it — so overlapping calls throw and sequential ones from
     * different threads do not. One lane gives exactly that: calls never overlap. It is not one thread,
     * and does not have to be; on `Dispatchers.IO`, a `poll` that waits holds a thread that exists for
     * waiting rather than the caller's.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private val lane = Dispatchers.IO.limitedParallelism(1)

    /** Where a `poll` runs, so that its caller can stop waiting for it — see [poll]. */
    private val polls = CoroutineScope(SupervisorJob() + lane)

    override suspend fun assign(partitions: List<TopicPartition>) {
        withContext(lane) { delegate.assign(partitions.map { it.apache() }) }
    }

    override suspend fun seek(
        partition: TopicPartition,
        to: SeekTo,
    ) {
        withContext(lane) {
            val apache = partition.apache()
            when (to) {
                SeekTo.Beginning -> {
                    delegate.seekToBeginning(listOf(apache))
                }

                SeekTo.End -> {
                    delegate.seekToEnd(listOf(apache))
                }

                is SeekTo.Offset -> {
                    delegate.seek(apache, to.offset)
                }

                is SeekTo.Timestamp -> {
                    val found: OffsetAndTimestamp? = delegate.offsetsForTimes(mapOf(apache to to.timestamp))[apache]
                    // No record at or after that time: the end, as the contract says.
                    if (found == null) delegate.seekToEnd(listOf(apache)) else delegate.seek(apache, found.offset())
                }
            }
        }
    }

    /**
     * Runs the Java `poll` on the lane, and lets the caller stop waiting for it.
     *
     * A blocked `poll` cannot see a coroutine's cancellation, so the caller's side calls `wakeup()` — the
     * one method the Java client allows from another thread — and the waiting `poll` throws
     * `WakeupException`. Interrupting the lane's thread is the rejected alternative: the client's own
     * documentation says it can abort a clean shutdown.
     *
     * Records the client returned to a `poll` whose caller was cancelled a moment later are not
     * handed to anyone: the position has moved past them. A caller who cancels a `poll` and wants
     * those records seeks back — which is what the contract says.
     *
     * `wakeup()` is sticky: called when no `poll` is waiting, it makes the NEXT one throw at once. So a
     * `WakeupException` reaching a `poll` whose caller is still active is a leftover from an earlier
     * cancellation, and that `poll` simply runs again.
     */
    override suspend fun poll(timeout: Duration): List<ConsumerRecord> {
        val call = polls.async { pollOnLane(timeout) }
        try {
            return call.await()
        } catch (cancelled: CancellationException) {
            // The waiting call first, then the client: cancelled, the lane's coroutine treats the
            // WakeupException that follows as the end rather than as a leftover to retry past.
            call.cancel()
            delegate.wakeup()
            throw cancelled
        }
    }

    private suspend fun pollOnLane(timeout: Duration): List<ConsumerRecord> {
        while (true) {
            try {
                return delegate.poll(timeout.toJavaDuration()).map { it.toKafkakn() }
            } catch (leftover: WakeupException) {
                currentCoroutineContext().ensureActive()
            }
        }
    }

    override suspend fun close() {
        withContext(lane) { delegate.close() }
        polls.cancel()
    }

    private fun TopicPartition.apache() = ApachePartition(topic, partition)

    private fun ApacheRecord<ByteArray?, ByteArray?>.toKafkakn() =
        ConsumerRecord(
            topic = topic(),
            partition = partition(),
            offset = offset(),
            timestamp = timestamp(),
            key = key(),
            value = value(),
            headers = headers().map { RecordHeader(it.key(), it.value()) },
        )
}
