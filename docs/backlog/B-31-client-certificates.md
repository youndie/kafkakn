---
id: B-31
title: "Client certificates: a broker that asks who is connecting gets an answer"
status: done
priority: P1
size: M
stage: stage-6-real-deployments
---

# B-31 — client certificates

TLS today proves the broker to the client. Many deployments also require the reverse — the broker set
to `ssl.client.auth=required` — and kafkakn cannot connect to them at all. [B-11](B-11-tls.md) left it
out on purpose; the owner's request of 2026-09-24 brings it in
([research §1.8](../research/research-architecture.md), D2 amended).

- **The decision and its reason.** librdkafka's spelling travels, as it does for the certificate
  authority: `ssl.certificate.location`, `ssl.key.location`, `ssl.key.password`. The JVM arm
  translates them, and **this translation is not a rename**: the Java client wants a key store, and
  whether its PEM key store can take two separate files or needs their contents is read out of
  `kafka-clients` 4.3.1 in this item, not assumed here.
- The broker fixture gains a listener that requires a client certificate, **beside** the two that
  exist rather than replacing either — the rule that kept TLS scenarios from quietly not running.
- The rejected alternative is taking the Java client's key-store keys as the contract's spelling. The
  contract has spelled TLS librdkafka's way since B-11; two conventions in one section is the mistake a
  caller makes on the arm they do not run locally.
- Not covered: certificate rotation without restarting the producer.

- AC: the right client certificate connects on both arms; no certificate is refused, and the message
  says so; a certificate signed by the wrong authority is refused — the three-scenario shape of
  `TlsTest`.
- AC: the translation is asserted directly, as `TranslateForJavaTest` asserts the other two.
- AC: the listener's own tools confirm the broker refuses a client without a certificate — the
  fixture is shown able to say no before anything it says yes to is read.
- Anchors: `kafkakn-core/src/jvmMain/kotlin/io/github/youndie/kafkakn/KafkaProducer.jvm.kt`,
  `kafkakn-core/src/commonMain/kotlin/io/github/youndie/kafkakn/TlsKeys.kt`,
  `ci/broker/docker-compose.tls.yml`, `ci/broker/certs.sh`.

## Findings (2026-09-24)

**The open question, answered by reading `kafka-clients` 4.3.1.** Its PEM key store takes a path only
as one file holding the chain and the key; two separate files reach it only as their contents
(`ssl.keystore.certificate.chain`, `ssl.keystore.key`). The JVM arm reads both at construction —
[research §2.20](../research/research-architecture.md) has the addresses.

**Measured, `ci/b-31/run.sh`:**

- the listener's own tools refused no certificate and a certificate from the wrong authority, then
  accepted the right one — before the suite ran;
- 200/200 records on each arm with an encrypted PKCS#8 key and `ssl.key.password`, counted over the
  plaintext listener by `kafka-console-consumer`;
- no certificate: both arms throw and name it — native *"tlsv13 alert certificate required"*, JVM
  *"(certificate_required) Received fatal alert"*;
- a certificate from the wrong authority: the **same** sentences, because neither client sends it —
  librdkafka's `rd_kafka_ssl_cert_callback` withholds a certificate whose issuer the server did not
  list, and the Java client was measured doing the same.

**Watched red first.** Before the translation the JVM arm refused `ssl.certificate.location` and
`ssl.key.location` as unknown keys, and `TranslateForJavaTest`'s three new cases failed on their
assertions. The native arm then showed what the item had not asked: **librdkafka constructs a producer
from a certificate with no key** (it checks the pair only when a key is set), so a new rule refuses
half a pair at construction on both arms — `MutualTlsTest` asserts on its message, because on the JVM
arm "it threw" had been green for the unknown-key reason.

**Left out, as new work.** A PKCS#1 key (`BEGIN RSA PRIVATE KEY`) reads in the source as working on
native and refused on the JVM; it is [B-42](B-42-a-pkcs1-key-works-on-one-arm.md), not measured here.
Certificate rotation stays out, as the item said.
