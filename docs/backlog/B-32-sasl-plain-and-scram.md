---
id: B-32
title: "SASL PLAIN and SCRAM: most managed Kafka will not talk to a client without them"
status: open
priority: P1
size: M
stage: stage-6-real-deployments
---

# B-32 — SASL PLAIN and SCRAM

Most hosted Kafka authenticates with SASL, so today kafkakn cannot connect to it. Both arms have what
is needed — `PLAIN` and `SCRAM` are compiled into our bundle, GSSAPI is not — and one of the three
spellings is already portable: librdkafka accepts **`sasl.mechanism`** as an alias of its own
`sasl.mechanisms` ([research §1.8](../research/research-architecture.md), read from `CONFIGURATION.md`).

- **The decision and its reason.** `sasl.mechanism`, `sasl.username` and `sasl.password` travel. The
  JVM arm builds `sasl.jaas.config` from the last two, and a caller who writes `sasl.jaas.config`
  themselves is left alone, as a caller who writes `ssl.truststore.location` is.
- **The JAAS string is where this will break, and the test says so first.** A password is quoted
  inside it, so a password containing a double quote or a backslash is the case a naive translation
  gets wrong — and gets wrong only on the JVM arm, where nothing else would notice.
- **Naming no mechanism is refused**, when `security.protocol` is `SASL_PLAINTEXT` or `SASL_SSL`. Both
  clients default to `GSSAPI`, which is not in our bundle, so the default is a failure on one arm and a
  Kerberos attempt on the other.
- The broker fixture gains SASL listeners, with PLAIN users and SCRAM credentials created through the
  broker's own tools.
- The rejected alternative is exposing `sasl.jaas.config` as the contract's only spelling. It is a
  Java string format, and making native callers write one is making them learn the other platform.
- Not covered: OAUTHBEARER ([B-33](B-33-sasl-oauthbearer.md)) and GSSAPI.

- AC: PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 connect on both arms with the right credentials; the
  wrong password is refused and the message names authentication.
- AC: a password containing a double quote and a backslash connects on both arms.
- AC: a SASL protocol with no mechanism is refused at construction, on both arms, with the key named.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`,
  `ci/broker/docker-compose.yml`, `ci/librdkafka/build.sh`, `docs/api/producer-contract.md`.
