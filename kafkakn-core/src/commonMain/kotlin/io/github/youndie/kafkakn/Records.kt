package io.github.youndie.kafkakn

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * The records this consumer reads, as a cold [Flow] built on [KafkaConsumer.poll]
 * ([B-54](../../../../../../../docs/backlog/B-54-a-flow-over-poll.md)). Consumer-contract §2 promised it as an
 * extension over `poll`, not instead of it, and it is exactly that. One implementation serves both arms.
 *
 * - **One collector at a time.** Collecting polls this consumer; two collectors would share its records
 *   between them. It never closes the consumer: whoever opened it closes it.
 * - **Cancelling the collector returns promptly**, and the consumer is usable afterwards: `poll`'s own
 *   promise (B-36).
 * - **The collector's pace is the poll's pace.** While the collector is busy with a record, nothing polls.
 *   A collector slower than `max.poll.interval.ms` is removed from its group, which is Kafka's rule, not
 *   this library's. Nothing in a `Flow`'s signature says so, and this is where it is said. Consumer-contract
 *   §2 describes what each arm then does.
 */
public fun KafkaConsumer.records(pollTimeout: Duration = DEFAULT_POLL_TIMEOUT): Flow<ConsumerRecord> =
    flow {
        while (true) {
            for (record in poll(pollTimeout)) emit(record)
        }
    }

private val DEFAULT_POLL_TIMEOUT = 500.milliseconds
