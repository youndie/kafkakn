---
id: feature-secure-connection
title: Connect to a broker over TLS
type: feature
status: active
owner: unassigned
involved_services:
  - kafkakn-core
  - test-broker
client_entries: []
api:
  - producer-contract
tags: [producer, security]
---

# Connect to a broker over TLS

## 1. Overview

A broker reachable over plaintext is not a normal deployment, so a producer that cannot speak TLS is
not usable. On the JVM this is the platform's TLS; on native it is the OpenSSL already linked into
the binary, which costs nothing additional at runtime — the measured binary's `ldd` set does not
change when TLS is turned on ([research §1.2](../research/research-architecture.md)).

**Built and measured 2026-09-17**: 500 records over the SSL listener on each arm, counted over the
plaintext one, and both arms refusing a broker signed by an authority they were not given.

## 2. Business rules

- Configuration is Kafka's own: `security.protocol=SSL`, `ssl.ca.location`.
- Certificate verification is **on**, and neither a convenience nor the raw key turns it off:
  `enable.ssl.certificate.verification` is refused at construction on both arms.
- Hostname checking can be relaxed, and that is the only part of TLS this API lets a caller weaken:
  `ssl.endpoint.identification.algorithm=none`.
- A peer that cannot be verified fails loudly, and the failure names certificate verification.
- SASL is not built yet. Since D2 was amended on 2026-09-24 it is planned — PLAIN and SCRAM in
  [B-32](../backlog/B-32-sasl-plain-and-scram.md), OAUTHBEARER in
  [B-33](../backlog/B-33-sasl-oauthbearer.md) — and client certificates in
  [B-31](../backlog/B-31-client-certificates.md).

## 3. Scenarios (BDD / test cases)

### Scenario: A record reaches the broker over TLS
* **Given:** the test broker with an SSL listener and a CA the client trusts.
* **When:** 500 records are sent over the SSL listener.
* **Then:** the topic's end offsets, **read over the plaintext listener**, have grown by exactly 500.
* **Automated:** `TlsTest.a_record_reaches_the_broker_over_tls`, counted by `ci/b-11/run.sh` through
  `kafka-console-consumer` on the plaintext port. Measured 500/500 on each arm.
* *The verification path is deliberately not the path under test.*

### Scenario: An untrusted peer is refused, and says why
* **Given:** a CA that did not sign the broker's certificate.
* **When:** a record is sent over the SSL listener.
* **Then:** the call throws, and the failure names certificate verification — not merely that the
  broker is unreachable, which is the same thing a closed port says.
* **Automated:** `TlsTest.an_untrusted_peer_is_refused_and_says_why`, asserting on the message and
  every cause. Measured — the native arm: *"SSL handshake failed: ... certificate verify failed:
  broker certificate could not be verified, verify that `ssl.ca.location` is correctly configured"*;
  the JVM arm: *"SSL handshake failed ... unable to find valid certification path to requested
  target"*.

### Scenario: The plaintext listener is not silently accepted as TLS
* **Given:** the plaintext listener.
* **When:** a TLS connection is attempted to it.
* **Then:** it fails as a protocol failure.
* **Automated:** `TlsTest.the_plaintext_listener_is_not_silently_accepted_as_tls`.
* *Without it, "TLS worked" cannot be told from "TLS was quietly not used".*

### Scenario: The key that would turn trust off is refused, on both arms
* **Given:** a configuration carrying `enable.ssl.certificate.verification`.
* **When:** a producer is constructed.
* **Then:** construction throws and the message names the key — on the native arm as well as on the
  JVM one, where the Java client had never heard of it anyway.
* **Automated:** `VerificationTest.turning_certificate_verification_off_is_refused_at_construction`
  and `…the_key_is_refused_even_where_it_asks_for_verification_to_stay_on`. Watched failing on the
  native arm before the rule existed: it accepted the key and constructed a producer.
* *The value does not matter. A key that constructs on one arm and throws on the other is the defect,
  whatever it is set to.*

### Scenario: Hostname checking can be relaxed, in one spelling, on both arms
* **Given:** `ssl.endpoint.identification.algorithm` set to `none`, and separately to `https`.
* **When:** a producer is constructed on each arm.
* **Then:** both construct; and the empty string the Java client documents is refused on both,
  because librdkafka cannot take it.
* **Automated:** `VerificationTest.hostname_verification_is_still_reachable_on_both_arms`,
  `…the_default_spelling_of_hostname_verification_is_accepted_too`,
  `…the_empty_value_kafka_clients_documents_is_refused_on_both_arms`, and
  `TranslateForJavaTest` for the `none` → `""` translation itself.
* *Construction succeeding does not prove the translation is right: `kafka-clients` accepts `none`
  as well and quietly enforces nothing, so the translation is asserted directly.*

## 4. Quirks

- **Nothing about TLS is proved by the fact that OpenSSL is linked.** A linked library that was
  never called is the same evidence as one that does not work; the spike published a size figure for
  TLS before anything had ever opened a TLS connection.
- **librdkafka's own error text ends "install ca-certificates package".** In a minimal container
  image there are none, and that is the error a user will see first.
- On native the CA is a file path (`ssl.ca.location`); on the JVM it is the trust store. The contract
  keeps librdkafka's spelling and the JVM arm translates it into `ssl.truststore.location` plus
  `ssl.truststore.type=PEM` (`translateForJava`) — the one place that arm is not a plain delegate.
  A caller who writes `ssl.truststore.location` themselves is left alone.
- **The two clients disagree about how to spell "off" for hostname checking, not about the key.**
  librdkafka refuses an empty value (*"cannot be set to empty value"*, measured 2026-09-17); the Java
  client documents the empty string. The contract takes `none` and translates. Worth knowing before
  copying a `ssl.endpoint.identification.algorithm=` line out of Kafka's documentation.
- **The two arms are not equally informative about the plaintext port.** Told to speak TLS to it,
  librdkafka says *"SSL connection closed by peer: connecting to a PLAINTEXT broker listener?"*,
  while the Java client reports only *"Topic ... not present in metadata after 20000 ms"*. Both fail,
  which is what the scenario requires; only one says why. Worth knowing before believing a metadata
  timeout.
- **A certificate that cannot be verified reaches the caller as `Local: Message timed out`** unless
  the producer keeps what the error callback said. `rd_kafka_produce` only enqueues, so the record is
  queued, retried and finally timed out; the sentence naming the certificate arrived on a different
  callback, minutes earlier ([research §2.9](../research/research-architecture.md)).
- **Certificate verification stays on, and the raw key is refused too** — on both arms, at
  construction, with the key named in the message
  ([B-18](../backlog/B-18-verification-cannot-be-turned-off.md)). This used to say
  `enable.ssl.certificate.verification` remained reachable for whoever insists. It does not, and the
  reason is the configuration rule rather than a view about security: the key exists only in
  librdkafka, so honouring it would mean the one way to switch trust off lived on the arm with no
  oracle.
- **Hostname checking is the exception, and it is named as one.**
  `ssl.endpoint.identification.algorithm` is on both arms and travels. Off is spelled **`none`**:
  librdkafka refuses an empty value outright — measured — while the Java client documents the empty
  string, so the JVM arm translates. It is the only remaining way to weaken TLS through this API and
  the contract says so out loud.

## 5. Code anchors

| What | Where |
|---|---|
| the TLS configuration path | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TlsTest.kt` |
| the broker's SSL overlay and certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
| the CA and hostname translations on the JVM arm | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt` |
| the two TLS keys and what each arm may do with them | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt` |
| what librdkafka last complained about | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the run that measured it | `ci/b-11/run.sh` |
