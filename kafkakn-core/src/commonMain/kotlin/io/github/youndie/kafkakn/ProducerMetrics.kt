package io.github.youndie.kafkakn

/**
 * What a producer's machinery is doing now, as an operator reads it
 * ([B-41](../../../../../../../docs/backlog/B-41-metrics-an-operator-can-read.md)).
 *
 * **What is here, and what is not, is the decision.** Each value describes the machinery — how much it
 * holds, how many requests it has out, how long the broker takes, how many brokers it is talking to.
 * None of them counts outcomes: a count of successes is the number that read "complete success" while a
 * quarter of the input had never been queued (research §1.4), and a build gate refuses one
 * (`scripts/no_delivery_counters.py`). Whether records arrived is the broker's to say.
 *
 * They are the same four NAMES on both arms and not quite the same four measurements: each client
 * samples and averages its own way, and [docs/api/producer-contract.md] says where the readings differ
 * and by how much (measured, B-41).
 *
 * Each is read from the arm's own client — `Producer.metrics()` on the JVM, the statistics librdkafka
 * emits every `statistics.interval.ms` on native — and a value is null where that source has not said
 * yet: the native arm's first statistics arrive one interval after the producer starts.
 */
public class ProducerMetrics(
    /** Bytes of records the client holds that the broker has not yet acknowledged or refused. */
    public val bufferedBytes: Long?,
    /** Produce requests sent to brokers and awaiting their response. */
    public val requestsInFlight: Int?,
    /**
     * The average round trip of recent requests, in milliseconds. "Recent" is each client's own window:
     * the Java client's metric window (thirty seconds by default), librdkafka's statistics interval — so
     * on native it is null whenever the last interval carried no request, an idle producer included.
     */
    public val brokerRoundTripMillis: Double?,
    /** Connections the client holds open, the bootstrap connection included where it is still open. */
    public val openConnections: Int?,
) {
    override fun toString(): String =
        "ProducerMetrics(bufferedBytes=$bufferedBytes, requestsInFlight=$requestsInFlight, " +
            "brokerRoundTripMillis=$brokerRoundTripMillis, openConnections=$openConnections)"
}
