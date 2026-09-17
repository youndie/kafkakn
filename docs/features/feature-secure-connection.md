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
- Certificate verification is **on**, and turning it off is not offered as a convenience.
- A peer that cannot be verified fails loudly, and the failure names certificate verification.
- SASL in every form is out of scope ([D2](../research/research-architecture.md)).

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
- **The two arms are not equally informative about the plaintext port.** Told to speak TLS to it,
  librdkafka says *"SSL connection closed by peer: connecting to a PLAINTEXT broker listener?"*,
  while the Java client reports only *"Topic ... not present in metadata after 20000 ms"*. Both fail,
  which is what the scenario requires; only one says why. Worth knowing before believing a metadata
  timeout.
- **A certificate that cannot be verified reaches the caller as `Local: Message timed out`** unless
  the producer keeps what the error callback said. `rd_kafka_produce` only enqueues, so the record is
  queued, retried and finally timed out; the sentence naming the certificate arrived on a different
  callback, minutes earlier ([research §2.9](../research/research-architecture.md)).
- **Certificate verification stays on**, and no named convenience turns it off. librdkafka's own
  `enable.ssl.certificate.verification` remains reachable as a raw configuration key for whoever
  insists — the point is that nothing in this library's vocabulary points at it.

## 5. Code anchors

| What | Where |
|---|---|
| the TLS configuration path | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TlsTest.kt` |
| the broker's SSL overlay and certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
| the CA translation on the JVM arm | `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt` |
| what librdkafka last complained about | `kafkakn-core/src/nativeMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.native.kt` |
| the run that measured it | `ci/b-11/run.sh` |
