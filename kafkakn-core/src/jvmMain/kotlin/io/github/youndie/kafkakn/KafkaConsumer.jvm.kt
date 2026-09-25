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
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.errors.FencedInstanceIdException
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

    private val namesAGroup = config.namesAGroup()

    init {
        // Before the strategy's translation, which would hand the Java client a class name it then refuses (B-57).
        config.checkGroupProtocolKeys()
    }

    private val properties =
        translateForJava(config.withContractDefaults()).let { translated ->
            // The portable words, as the Java client's class names (B-55).
            val strategy = config.assignmentStrategy() ?: return@let translated
            translated +
                ("partition.assignment.strategy" to strategy.joinToString(",") { JAVA_ASSIGNORS.getValue(it) })
        }

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

    /**
     * Whether this thread is inside a rebalance callback right now (consumer-contract §2a). The callback
     * runs on the lane's thread, inside `poll` or `close`; a call to this consumer from there would wait
     * on the lane the callback is holding. Per thread, so a call from any other coroutine still queues.
     */
    private val inCallback = ThreadLocal.withInitial { false }

    private fun enter(call: String) {
        if (inCallback.get()) refuseReentry(call)
    }

    override suspend fun assign(partitions: List<TopicPartition>) {
        enter("assign")
        withContext(lane) { delegate.assign(partitions.map { it.apache() }) }
        subscribed = false
    }

    /** Whether [subscribe] was the last of the two ways to get partitions. */
    @Volatile
    private var subscribed = false

    override suspend fun subscribe(topics: List<String>) {
        enter("subscribe")
        requireGroup(namesAGroup, "subscribe")
        // No rebalance listener: with auto-commit off and nothing to flush on revocation, the Java
        // client's own handling is the at-least-once the contract promises - a revoked partition
        // resumes elsewhere from its last commit.
        withContext(lane) { delegate.subscribe(topics) }
        subscribed = true
    }

    override suspend fun subscribe(
        topics: List<String>,
        listener: RebalanceListener,
    ) {
        enter("subscribe")
        requireGroup(namesAGroup, "subscribe")
        withContext(lane) { delegate.subscribe(topics, bridge(listener)) }
        subscribed = true
    }

    /**
     * The Java listener, calling ours. Each callback runs on the thread inside `poll` or `close`, which
     * is already on the lane, so the scope commits with `commitSync` directly: going through the lane
     * again is the deadlock §2a is about. `onPartitionsLost` is overridden, so it does not fall through
     * to `onPartitionsRevoked` as its default would. Empty lists are not passed on (§2a).
     */
    private fun bridge(listener: RebalanceListener): ConsumerRebalanceListener {
        fun scope(assigning: Boolean) =
            object : RebalanceScope {
                override fun commit(offsets: Map<TopicPartition, Long>) {
                    requireCommittable(offsets)
                    if (offsets.isEmpty()) return
                    delegate.commitSync(offsets.entries.associate { it.key.apache() to OffsetAndMetadata(it.value) })
                }

                override fun seek(
                    partition: TopicPartition,
                    to: SeekTo,
                ) {
                    check(assigning) { "seek from onRevoked: $partition is leaving this member" }
                    seekNow(partition, to)
                }
            }
        return object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<ApachePartition>) =
                inside(partitions) { listener.onRevoked(it, scope(assigning = false)) }

            override fun onPartitionsAssigned(partitions: Collection<ApachePartition>) =
                inside(partitions) { listener.onAssigned(it, scope(assigning = true)) }

            override fun onPartitionsLost(partitions: Collection<ApachePartition>) =
                inside(partitions, listener::onLost)
        }
    }

    private fun inside(
        partitions: Collection<ApachePartition>,
        call: (List<TopicPartition>) -> Unit,
    ) {
        if (partitions.isEmpty()) return
        inCallback.set(true)
        try {
            call(partitions.map { TopicPartition(it.topic(), it.partition()) }.sortedWith(PARTITION_ORDER))
        } finally {
            inCallback.set(false)
        }
    }

    override suspend fun commit() {
        enter("commit")
        requireGroup(namesAGroup, "commit")
        // commitSync with no arguments: the positions after everything `poll` has returned, for every
        // partition held. It waits for the coordinator, on the lane.
        withContext(lane) { delegate.commitSync() }
    }

    override suspend fun commit(offsets: Map<TopicPartition, Long>) {
        enter("commit")
        requireGroup(namesAGroup, "commit")
        requireCommittable(offsets)
        if (offsets.isEmpty()) return
        // commitSync(Map): the offsets as given, each the next one to read, which is also what
        // OffsetAndMetadata means. No metadata string: nothing here needs one (B-48).
        val apache = offsets.entries.associate { it.key.apache() to OffsetAndMetadata(it.value) }
        withContext(lane) { delegate.commitSync(apache) }
    }

    override suspend fun groupMetadata(): ConsumerGroupMetadata {
        enter("groupMetadata")
        requireGroup(namesAGroup, "groupMetadata")
        return JvmGroupMetadata(withContext(lane) { delegate.groupMetadata() })
    }

    // position and committed may each wait for the broker, as the Java client allows (up to
    // default.api.timeout.ms), and they do it on the lane, where no other call is running.
    override suspend fun position(partition: TopicPartition): Long {
        enter("position")
        return withContext(lane) { delegate.position(partition.apache()) }
    }

    override suspend fun committed(partitions: List<TopicPartition>): Map<TopicPartition, Long?> {
        enter("committed")
        requireGroup(namesAGroup, "committed")
        val answer = withContext(lane) { delegate.committed(partitions.map { it.apache() }.toSet()) }
        return partitions.associateWith { answer[it.apache()]?.offset() }
    }

    override suspend fun pause(partitions: List<TopicPartition>) {
        enter("pause")
        withContext(lane) { delegate.pause(held(partitions, "pause")) }
    }

    override suspend fun resume(partitions: List<TopicPartition>) {
        enter("resume")
        withContext(lane) { delegate.resume(held(partitions, "resume")) }
    }

    override suspend fun paused(): List<TopicPartition> {
        enter("paused")
        return withContext(lane) {
            delegate.paused().map { TopicPartition(it.topic(), it.partition()) }.sortedWith(PARTITION_ORDER)
        }
    }

    /** [partitions] as the Java client's, refused in the contract's words if any is not held. */
    private fun held(
        partitions: List<TopicPartition>,
        call: String,
    ): List<ApachePartition> {
        val holding = delegate.assignment()
        return partitions.map { it.apache() }.onEach {
            check(
                it in holding,
            ) { "$call: $it is not held by this consumer" }
        }
    }

    /** `currentLag`: the end the last fetch saw, minus the position; empty until the partition's first fetch. */
    override suspend fun metrics(): ConsumerMetrics {
        enter("metrics")
        return withContext(lane) {
            ConsumerMetrics(
                delegate
                    .assignment()
                    .map { TopicPartition(it.topic(), it.partition()) }
                    .sortedWith(PARTITION_ORDER)
                    .associateWith { partition ->
                        val lag = delegate.currentLag(partition.apache())
                        if (lag.isPresent) lag.asLong else null
                    },
            )
        }
    }

    override suspend fun assignment(): List<TopicPartition> {
        enter("assignment")
        return withContext(lane) {
            delegate.assignment().map { TopicPartition(it.topic(), it.partition()) }.sortedWith(PARTITION_ORDER)
        }
    }

    override suspend fun seek(
        partition: TopicPartition,
        to: SeekTo,
    ) {
        enter("seek")
        withContext(lane) { seekNow(partition, to) }
    }

    /**
     * The seek itself, for a caller already on the lane: [seek], and the scope inside `onAssigned`, which
     * runs on the lane's thread inside `poll` (B-51). A partition not held is refused here, in the
     * contract's words, rather than left to the Java client's own message.
     */
    private fun seekNow(
        partition: TopicPartition,
        to: SeekTo,
    ) {
        val apache = partition.apache()
        check(apache in delegate.assignment()) { "seek: $partition is not held by this consumer" }
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
        enter("poll")
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
            } catch (fenced: FencedInstanceIdException) {
                throw ConsumerFencedException("poll: fenced by a member with the same group.instance.id", fenced)
            }
        }
    }

    override suspend fun close() {
        enter("close")
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

/** The Java consumer's own group metadata, carried to the producer untouched (B-38). */
internal class JvmGroupMetadata(
    val apache: org.apache.kafka.clients.consumer.ConsumerGroupMetadata,
) : ConsumerGroupMetadata() {
    override val groupId: String get() = apache.groupId()
}

/** The Java client's class for each portable assignor word (B-55). */
private val JAVA_ASSIGNORS: Map<String, String> =
    mapOf(
        "range" to "org.apache.kafka.clients.consumer.RangeAssignor",
        "roundrobin" to "org.apache.kafka.clients.consumer.RoundRobinAssignor",
        "cooperative-sticky" to "org.apache.kafka.clients.consumer.CooperativeStickyAssignor",
    )
