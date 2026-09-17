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

**`status: draft`: nothing here is built.** Every scenario is *target*.

## 2. Business rules

- Configuration is Kafka's own: `security.protocol=SSL`, `ssl.ca.location`.
- Certificate verification is **on**, and turning it off is not offered as a convenience.
- A peer that cannot be verified fails loudly, and the failure names certificate verification.
- SASL in every form is out of scope ([D2](../research/research-architecture.md)).

## 3. Scenarios (BDD / test cases)

Every one is **target**: nothing is built.

### Scenario: A record reaches the broker over TLS
* **Given:** the test broker with an SSL listener and a CA the client trusts.
* **When:** 500 records are sent over the SSL listener.
* **Then:** the topic's end offsets, **read over the plaintext listener**, have grown by exactly 500.
* *The verification path is deliberately not the path under test.*

### Scenario: An untrusted peer is refused, and says why
* **Given:** a CA that did not sign the broker's certificate.
* **When:** a record is sent over the SSL listener.
* **Then:** the call throws, and the failure names certificate verification — not merely that the
  broker is unreachable, which is the same thing a closed port says.

### Scenario: The plaintext listener is not silently accepted as TLS
* **Given:** the plaintext listener.
* **When:** a TLS connection is attempted to it.
* **Then:** it fails as a protocol failure.
* *Without it, "TLS worked" cannot be told from "TLS was quietly not used".*

## 4. Quirks

- **Nothing about TLS is proved by the fact that OpenSSL is linked.** A linked library that was
  never called is the same evidence as one that does not work; the spike published a size figure for
  TLS before anything had ever opened a TLS connection.
- **librdkafka's own error text ends "install ca-certificates package".** In a minimal container
  image there are none, and that is the error a user will see first.
- On native the CA is a file path (`ssl.ca.location`); on the JVM it is the trust store. The contract
  hides the difference and the suite checks that it hides it faithfully.

## 5. Code anchors

| What | Where |
|---|---|
| the TLS configuration path | `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/ProducerConfig.kt` |
| the scenarios above | `kafkakn-core/src/commonTest/kotlin/io/github/youndie/kafkakn/TlsTest.kt` |
| the broker's SSL overlay and certificates | `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh` |
