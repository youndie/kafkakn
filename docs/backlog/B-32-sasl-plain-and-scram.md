---
id: B-32
title: "SASL PLAIN and SCRAM: most managed Kafka will not talk to a client without them"
status: done
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

## Findings (2026-09-24)

**Measured, `ci/b-32/run.sh`.** The broker's own tools refused a wrong password for PLAIN and for
SCRAM-SHA-256 before the suite ran. Then, on each arm, 100/100 records for PLAIN, SCRAM-SHA-256,
SCRAM-SHA-512 and SCRAM-SHA-512 over `SASL_SSL`, counted over plaintext by `kafka-console-consumer`;
and 100/100 for the password holding a double quote and a backslash. The wrong password is refused on
both arms and named: JVM *"Authentication failed: Invalid username or password"*, native the same
sentence behind *"Local: Authentication failure"*.

**The JAAS string, checked by the Java client's own parser.** `TranslateForJavaTest` reads each
password back through `JaasContext.loadClientContext` — a double quote, a trailing backslash, line
breaks and a tab — rather than comparing strings. The native arm sends the quoted password raw, so its
getting in is what says the broker's JAAS file holds the password the test means.

**Watched red first.** The JVM arm refused `sasl.username`/`sasl.password` as unknown keys; the
native arm connected everything and failed only the two construction rules — librdkafka refused them
itself, as *"No provider for SASL mechanism GSSAPI"* and *"sasl.username and sasl.password must be
set"*, neither naming the key. A third rule came with them: credentials for a mechanism that takes
none (GSSAPI, or none named) are refused on both arms, because librdkafka drops them and the JVM arm
has no login module to put them in.

**On the way.** Both SCRAM mechanisms in one `kafka-configs --add-config` are refused by the broker
(*"A user credential cannot be altered twice in the same request"*); the fixture creates them one per
call. Research §2.21 has the addresses.
