package io.github.youndie.kafkakn.schema

/** The arm this suite runs on, for names that must not collide between the two. */
internal expect val arm: String

/** An environment variable, for what the runner hands the suite. */
internal expect fun env(name: String): String?

/** The fixture registry, which `ci/harness/broker.sh up` brings with the broker. */
internal val registryUrl: String get() = env("KAFKAKN_REGISTRY") ?: "http://127.0.0.1:18081"

/** This run's name, so the runner can find the run's own requests in the registry's log. */
internal val run: String get() = env("KAFKAKN_RUN") ?: "local"
