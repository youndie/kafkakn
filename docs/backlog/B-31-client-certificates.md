---
id: B-31
title: "Client certificates: a broker that asks who is connecting gets an answer"
status: wip
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
