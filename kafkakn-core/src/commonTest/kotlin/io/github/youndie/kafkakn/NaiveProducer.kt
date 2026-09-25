package io.github.youndie.kafkakn

import kotlinx.coroutines.sync.Semaphore

/**
 * The defect, on purpose: a producer that counts an enqueue refusal and moves on.
 *
 * It exists so that [AccountingTest] can be shown **failing**. A guard written against a defect that
 * is never reproduced is a guard nobody has seen work, and in this repository that is not a
 * theoretical worry: four checks have passed here while testing nothing at all.
 *
 * The shape is the measured one ([research §1.4](../../../../../../../docs/research/research-architecture.md)):
 * a naive binding called `rd_kafka_produce`, was refused with `QUEUE_FULL` when the outbound queue
 * was full, treated that as "this one did not fit", and went on to the next record. It lost
 * **264 826 of 1 000 000** records while every indicator it had stayed green — because a record that
 * was never queued produces no delivery report, so nothing it could count was ever wrong.
 *
 * **What is faithful here and what is not.** The refusal is simulated with a permit count rather
 * than taken from librdkafka, so this is not a second binding and says nothing about librdkafka's
 * own refusal path. What it reproduces exactly is the condition the guard exists to catch: records
 * the caller handed in, for which `send` returned a result-shaped value, that no broker ever saw.
 * That is the whole of what [AccountingTest] asserts against, and it is a better control for being
 * identical on both arms — a control that only one arm can run leaves the other arm's guard
 * unproven.
 *
 * [NAIVE_DROP_PARTITION] is what makes the loss visible from inside the test. The original could not
 * see it at all, which is the point; a control that hid it as well would only prove the guard is
 * needed, not that it works.
 */
internal class NaiveProducer(
    private val delegate: KafkaProducer,
    bound: Int,
) : KafkaProducer {
    private val room = Semaphore(bound)

    override suspend fun send(record: ProducerRecord): RecordMetadata {
        if (!room.tryAcquire()) {
            // "The queue is full, so this one did not fit." Returned, not thrown - the caller has a
            // RecordMetadata in their hand and no reason to look at it.
            return RecordMetadata(record.topic, NAIVE_DROP_PARTITION, NAIVE_DROP_OFFSET, NAIVE_DROP_OFFSET)
        }
        try {
            return delegate.send(record)
        } finally {
            room.release()
        }
    }

    override suspend fun partitionsFor(topic: String): List<PartitionInfo> = delegate.partitionsFor(topic)

    override suspend fun initTransactions() = delegate.initTransactions()

    override suspend fun beginTransaction() = delegate.beginTransaction()

    override suspend fun sendOffsetsToTransaction(
        offsets: Map<TopicPartition, Long>,
        group: ConsumerGroupMetadata,
    ) = delegate.sendOffsetsToTransaction(offsets, group)

    override suspend fun commitTransaction() = delegate.commitTransaction()

    override suspend fun abortTransaction() = delegate.abortTransaction()

    override suspend fun metrics() = delegate.metrics()

    override suspend fun flush() = delegate.flush()

    override suspend fun close() = delegate.close()
}

/**
 * A partition number no broker can hand out, so a test can count dropped records without having to
 * trust the producer that dropped them.
 */
internal const val NAIVE_DROP_PARTITION: Int = -1

internal const val NAIVE_DROP_OFFSET: Long = -1L
