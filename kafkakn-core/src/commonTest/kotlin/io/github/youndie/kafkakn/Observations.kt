package io.github.youndie.kafkakn

/**
 * How one arm tells the other what it saw.
 *
 * The two arms cannot compare notes inside a test: they are separate processes on separate
 * platforms, and `commonTest` is compiled twice rather than run once. So a differential oracle needs
 * somewhere outside both of them — each arm records what it observed, and a step after the suite
 * diffs the records.
 *
 * Most assertions do not need this. Where the truth is the **broker's** — how many records landed,
 * on which partition, at which offset — each arm is checked against it independently and agreement
 * follows from both being right. What needs recording is what only the client knows: which partition
 * *its own* partitioner chose for a key, what its error for a given failure was, how it rounded a
 * configuration value. Those are exactly the places the two implementations can differ while both
 * look correct on their own.
 *
 * The file is per-arm and the comparison is `ci/harness/compare-arms.sh`.
 */
internal expect fun recordObservation(
    key: String,
    value: String,
)

/**
 * A fact that belongs to **this** arm and is not expected to match the other.
 *
 * Stamps and counts are per-arm by construction — each run makes its own — so comparing them would
 * report a disagreement on every run and the comparison would stop being read. They go to a separate
 * file, which the per-item scripts use to ask the broker what happened.
 *
 * The split is the point: a file where everything must agree is a file whose disagreements mean
 * something.
 */
internal expect fun recordArmFact(
    key: String,
    value: String,
)

/** The arm this test is running on, as it names itself in the observation file. */
internal expect val armName: String

/** One environment variable, read the same way on both arms. */
internal expect fun testEnv(name: String): String?

/** Where the suite's broker is. The harness always sets it; the default is for a local run. */
internal val bootstrap: String get() = testEnv("KAFKAKN_BOOTSTRAP") ?: "127.0.0.1:9092"

/** The topic the harness created. Never auto-created: the broker has auto-creation off. */
internal val testTopic: String get() = testEnv("KAFKAKN_TOPIC") ?: "kafkakn"

/**
 * A producer configuration whose outbound queue is small enough that a test can fill it.
 *
 * **The two clients do not name this knob the same way**, and there is no common spelling to give
 * them: librdkafka bounds the queue by `queue.buffering.max.messages`, while the Java client bounds
 * it by `buffer.memory` in bytes and waits `max.block.ms` for room. The contract keeps Kafka's own
 * names rather than inventing a third, so "make the queue small" is necessarily per-arm — and this
 * is the seam where that shows.
 *
 * The default bound is 100 000 records; a test that does not lower it never reaches the case that
 * matters and passes for the wrong reason.
 */
internal expect fun smallQueueConfig(): Map<String, String>

/** How many times a caller has had to wait for room. Zero means the backpressure path never ran. */
internal expect fun backpressureWaitCount(): Long

/**
 * The topic this arm accounts on, **per arm and fresh for the run**.
 *
 * The accounting oracle is the topic's end offsets, and a delta is only attributable while nothing
 * else writes to the topic. Both arms share one broker and one run, so they cannot share a topic
 * without their two counts adding up into one number that no single assertion can check. The suffix
 * is [armName] and the harness creates both.
 *
 * **There is no default, and there used to be one.** It read `"kafkakn-acct"`, which is a name
 * nothing creates: `ci/b-09/run.sh` passes a per-run one. A test started any other way therefore
 * asked for a topic that could not exist, the Java client waited `max.block.ms` for metadata that
 * was never coming, and `runTest`'s own one-minute watchdog fired first — so the central guard of
 * this project went red after sixty seconds of silence with a sentence about **coroutines**
 * ([B-24](../../../../../../../docs/backlog/B-24-the-central-guard-times-out.md)). It was filed as a
 * timeout under load and it was neither.
 *
 * `testTopic` and `strictTopic` keep their defaults on purpose: those fall back to exactly the names
 * the scripts use, so they name something that exists. This one named something that does not.
 */
internal val accountingTopic: String get() {
    val provided =
        testEnv("KAFKAKN_ACCOUNTING_TOPIC")
            ?: error(
                "KAFKAKN_ACCOUNTING_TOPIC is unset, so this test was not started by ci/b-09/run.sh. " +
                    "That script creates the topic this test accounts on, fresh for the run and one " +
                    "per arm, and reads the end offsets afterwards - the half of the assertion that " +
                    "does not live in the test. Run: bash ci/b-09/run.sh",
            )
    return "$provided-$armName"
}

/**
 * Whether this run is the deliberate positive control.
 *
 * Set by `ci/b-09/run.sh` for a second pass in which the accounting test runs against
 * [NaiveProducer] and **must fail**. An environment variable rather than a system property: `-D` on
 * the Gradle command line sets a property on the Gradle process and never reaches the forked test
 * JVM, which is how an earlier skew guard was steered into agreeing with itself (research §2.1).
 */
internal fun naiveProducerRequested(): Boolean = testEnv("KAFKAKN_NAIVE") == "1"

/**
 * A topic nothing can be acknowledged on: `min.insync.replicas=2` on a single-broker cluster.
 *
 * The fixture for every question that only has an answer while a producer is stuck - a buffer that
 * drains never fills, and a test against the ordinary topic measures how fast the broker is rather
 * than what the client does when it cannot proceed.
 */
internal val strictTopic: String get() = testEnv("KAFKAKN_STRICT_TOPIC") ?: "kafkakn-strict"

/**
 * A topic whose broker keeps **its own** clock: `message.timestamp.type=LogAppendTime`.
 *
 * Created by `ci/harness/broker.sh up` itself rather than by each script that runs the suite — six
 * of them do, and a seventh topic added to six hand-written lists is the list that goes stale. The
 * default therefore names something the fixture always creates, which is the condition a default has
 * to meet here since B-24.
 */
internal val logAppendTopic: String get() = testEnv("KAFKAKN_LOGAPPEND_TOPIC") ?: "kafkakn-logappend"

/**
 * The broker's TLS listener, which sits **beside** the plaintext one rather than replacing it.
 *
 * Both are always up (`ci/harness/broker.sh up`). A fixture with a plaintext mode and a TLS mode
 * would give the suite a mode in which its TLS scenarios quietly do not run, and a scenario that did
 * not run reads exactly like one that passed.
 */
internal val sslBootstrap: String get() = testEnv("KAFKAKN_SSL_BOOTSTRAP") ?: "127.0.0.1:9094"

/** The certificate authority that signed the broker's certificate. */
internal val caPath: String get() = testEnv("KAFKAKN_CA") ?: "${testEnv("HOME")}/.cache/kafkakn/tls/ca.pem"

/**
 * An authority that signed nothing here.
 *
 * Without it a TLS test cannot tell "the certificate was verified" from "verification never
 * happened" — and the second is also what a client with verification disabled looks like.
 */
internal val wrongCaPath: String get() =
    testEnv("KAFKAKN_WRONG_CA")
        ?: "${testEnv("HOME")}/.cache/kafkakn/tls/wrong-ca.pem"

/**
 * Timeouts short enough that a connection which will never succeed fails inside a test.
 *
 * Per-arm for the same reason as [smallQueueConfig]: librdkafka gives up on a record after
 * `message.timeout.ms`, the Java client after `delivery.timeout.ms` and only after `max.block.ms`
 * of waiting for metadata. The defaults are minutes, and a negative TLS scenario spends every one
 * of them before saying anything.
 */
internal expect fun failFastConfig(): Map<String, String>

/**
 * Whether a producer built from [config] is **actually** idempotent, read from the client underneath
 * rather than from what kafkakn asked it for — the Java client's own post-processed configuration on
 * one arm, `rd_kafka_conf_get` on a constructed handle on the other. Throws when the producer refuses
 * to be constructed.
 */
internal expect fun effectiveIdempotence(config: ProducerConfig): Boolean
