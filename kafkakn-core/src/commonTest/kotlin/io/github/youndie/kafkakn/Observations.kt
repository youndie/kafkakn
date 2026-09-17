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
internal expect fun recordObservation(key: String, value: String)

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
internal expect fun recordArmFact(key: String, value: String)

/** The arm this test is running on, as it names itself in the observation file. */
internal expect val armName: String

/** One environment variable, read the same way on both arms. */
internal expect fun testEnv(name: String): String?

/** Where the suite's broker is. The harness always sets it; the default is for a local run. */
internal val bootstrap: String get() = testEnv("KAFKAKN_BOOTSTRAP") ?: "127.0.0.1:9092"

/** The topic the harness created. Never auto-created: the broker has auto-creation off. */
internal val testTopic: String get() = testEnv("KAFKAKN_TOPIC") ?: "kafkakn"
